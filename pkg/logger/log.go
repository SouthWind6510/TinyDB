package logger

import "github.com/sirupsen/logrus"

var Log = InitLogger()

func InitLogger() (log *logrus.Logger) {
	log = logrus.New()
	log.SetReportCaller(true)
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
		ForceColors:     true,
	})
	return
}
