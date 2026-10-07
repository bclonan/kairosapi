package orchestrator

import (
	"errors"
	"sort"
	"time"

	"github.com/bclonan/kairosapi/internal/artifact"
	"github.com/bclonan/kairosapi/internal/identity"
	"github.com/bclonan/kairosapi/internal/workflow"
	bolt "go.etcd.io/bbolt"
)

// Format 3 adds the transition journal and checkpoint recovery.
// Migration preserves existing public run IDs and commits all changes together.
func (s *Service) initialize() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists([]byte("meta"))
		if err != nil {
			return err
		}
		version := string(meta.Get([]byte("format")))
		if version != "" && version != "1" && version != "2" && version != "3" {
			return errors.New("unsupported database format")
		}
		for _, name := range [][]byte{definitionsBucket, runsBucket, pendingBucket, receiptsBucket, expiryBucket, orderBucket, summariesBucket, publicationsBucket} {
			if (version == "2" || version == "3") && tx.Bucket(name) == nil {
				return errors.New("required database index is missing")
			}
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		if err := artifact.Init(tx); err != nil {
			return err
		}
		if version == "3" && tx.Bucket(observationsBucket) == nil {
			return errors.New("required transition journal is missing")
		}
		if err := initializeObservability(tx); err != nil {
			return err
		}
		if version == "" || version == "1" {
			var records []Run
			if err := tx.Bucket(runsBucket).ForEach(func(_, value []byte) error {
				var record storedRun
				if err := workflow.Decode(value, &record); err != nil {
					return err
				}
				summary := record.Run
				summary.Steps, summary.Output = nil, nil
				records = append(records, summary)
				return nil
			}); err != nil {
				return err
			}
			sort.Slice(records, func(i, j int) bool {
				if records[i].CreatedAt.Equal(records[j].CreatedAt) {
					return records[i].ID < records[j].ID
				}
				return records[i].CreatedAt.Before(records[j].CreatedAt)
			})
			for _, run := range records {
				id, err := identity.Next(tx, run.CreatedAt)
				if err != nil {
					return err
				}
				run.SequenceID = id
				if err := saveRunIndex(tx, run); err != nil {
					return err
				}
				// Completed result bodies stay untouched. Only queue entries need
				// their stored scheduling key rewritten during this migration.
				if run.State == "queued" {
					record, err := readRun(tx, run.ID)
					if err != nil {
						return err
					}
					if err := tx.Bucket(pendingBucket).Delete([]byte(record.PendingKey)); err != nil {
						return err
					}
					record.PendingKey = id
					if err := tx.Bucket(pendingBucket).Put([]byte(id), []byte(record.Run.ID)); err != nil {
						return err
					}
					if err := saveRun(tx, record); err != nil {
						return err
					}
				} else if terminal(run.State) {
					if err := tx.Bucket(expiryBucket).Put([]byte(timeKey(run.FinishedAt)+"/run/"+run.ID), []byte(run.ID)); err != nil {
						return err
					}
				}
			}
		}
		if err := meta.Put([]byte("format"), []byte("3")); err != nil {
			return err
		}
		return nil
	})
}

func (s *Service) verifyIndexes() error {
	return s.db.View(func(tx *bolt.Tx) error {
		last, err := identity.Last(tx)
		if err != nil {
			return err
		}
		invalid := errors.New("run or publication index is inconsistent")
		queued := 0
		if err := tx.Bucket(runsBucket).ForEach(func(key, _ []byte) error {
			record, err := readRun(tx, string(key))
			if err != nil {
				return err
			}
			run := record.Run
			switch run.State {
			case "queued", "running", "succeeded", "failed", "timed_out", "canceled", "interrupted":
			default:
				return invalid
			}
			if string(key) != run.ID || !identity.Valid(run.SequenceID) || run.SequenceID > last || string(tx.Bucket(orderBucket).Get([]byte(run.SequenceID))) != run.ID {
				return invalid
			}
			var summary Run
			if workflow.Decode(tx.Bucket(summariesBucket).Get(key), &summary) != nil || summary.ID != run.ID || summary.WorkflowID != run.WorkflowID || summary.Version != run.Version || summary.SequenceID != run.SequenceID || summary.State != run.State {
				return invalid
			}
			if run.State == "queued" {
				queued++
				if record.PendingKey != run.SequenceID || string(tx.Bucket(pendingBucket).Get([]byte(record.PendingKey))) != run.ID {
					return invalid
				}
			}
			return nil
		}); err != nil {
			return err
		}
		count := tx.Bucket(runsBucket).Stats().KeyN
		if count != tx.Bucket(orderBucket).Stats().KeyN || count != tx.Bucket(summariesBucket).Stats().KeyN || queued != tx.Bucket(pendingBucket).Stats().KeyN {
			return invalid
		}
		if err := tx.Bucket(publicationsBucket).ForEach(func(key, id []byte) error {
			if !identity.Valid(string(id)) || string(id) > last || tx.Bucket(definitionsBucket).Get(key) == nil {
				return invalid
			}
			return nil
		}); err != nil {
			return err
		}
		return verifyObservability(tx)
	})
}

func publication(tx *bolt.Tx, key string) (string, error) {
	if id := tx.Bucket(publicationsBucket).Get([]byte(key)); id != nil {
		return string(id), nil
	}
	id, err := identity.Next(tx, time.Now())
	if err != nil {
		return "", err
	}
	return id, tx.Bucket(publicationsBucket).Put([]byte(key), []byte(id))
}
