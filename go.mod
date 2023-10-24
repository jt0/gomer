module github.com/jt0/gomer

go 1.21

//require (
//	github.com/aws/aws-sdk-go v1.38.15
//	github.com/gin-gonic/gin v1.8.1
//)

require (
	github.com/aws/aws-sdk-go v0.0.0-00010101000000-000000000000
	github.com/gin-gonic/gin v0.0.0-00010101000000-000000000000
)

replace github.com/aws/aws-sdk-go v0.0.0-00010101000000-000000000000  => /Volumes/workplace/aws-brewski/build/bgospace/AWSGoSDKInternal-1.0/src/github.com/aws/aws-sdk-go
replace github.com/gin-gonic/gin v0.0.0-00010101000000-000000000000  => /Volumes/workplace/aws-brewski/build/bgospace/Go3p-Github-GinGonic-Gin-1.x/src/github.com/gin-gonic/gin
