package status

import "github.com/gin-gonic/gin"

func IsUp(c *gin.Context) {
	c.String(200, "true")
}
