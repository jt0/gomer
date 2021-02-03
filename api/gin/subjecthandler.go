package gin

import (
	"github.com/gin-gonic/gin"

	"github.com/jt0/gomer/auth"
	"github.com/jt0/gomer/gomerr"
)

var Subject = NilSubject

func NilSubject(*gin.Context) auth.Subject {
	return nil
}

type SubjectProvider func(*gin.Context) (auth.Subject, gomerr.Gomerr)

func SubjectHandler(subjectProvider SubjectProvider) gin.HandlerFunc {
	if subjectProvider == nil {
		subjectProvider = func(*gin.Context) (auth.Subject, gomerr.Gomerr) {
			return auth.NewSubject(auth.NoFieldAccess), nil
		}
	}

	Subject = SubjectHandlerSubject

	return func(c *gin.Context) {
		if subject, ge := subjectProvider(c); ge != nil {
			_ = c.Error(ge)
			c.Abort()
		} else {
			c.Set(SubjectKey, subject)
			c.Next()
			ge := subject.Release(c.IsAborted() || len(c.Errors) > 0)
			if ge != nil {
				// TODO: log but don't error
			}
		}
	}
}

const SubjectKey = "gomer-subject"

func SubjectHandlerSubject(c *gin.Context) auth.Subject {
	return c.MustGet(SubjectKey).(auth.Subject)
}
