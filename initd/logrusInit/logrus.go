// Package logrusInit
/**
 * @author:WXZ
 * @date:2021/8/31
 * @note
 */
package logrusInit

import (
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"log"
	"os"
	path2 "path"
	"path/filepath"
	"sample_search_export/conststat"
	"time"
)

func New() {
	if viper.GetBool("log.use_json") {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	}

	// log.logger_level
	switch viper.GetString("log.logger_level") {
	case "trace":
		logrus.SetLevel(logrus.TraceLevel)
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "warn":
		logrus.SetLevel(logrus.WarnLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	}

	devNull, _ := os.OpenFile(os.DevNull, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	logrus.SetOutput(devNull)

	// log.logger_file
	logrus_info_file := viper.GetString("log.logger_info_file")
	logrus_info_file = path2.Join(conststat.ROOT_PATH, logrus_info_file)
	os.MkdirAll(filepath.Dir(logrus_info_file), os.ModePerm)

	logrus_error_file := viper.GetString("log.logger_error_file")
	logrus_error_file = path2.Join(conststat.ROOT_PATH, logrus_error_file)
	os.MkdirAll(filepath.Dir(logrus_error_file), os.ModePerm)

	// 设置 rotatelogs
	info_file_writer, err := rotatelogs.New(
		// 分割后的文件名称
		logrus_info_file+".%Y%m%d.log",

		// 生成软链，指向最新日志文件,windows下禁止创建软连了
		//rotatelogs.WithLinkName(logrus_info_file),

		// 设置最大保存时间(7天)
		rotatelogs.WithMaxAge(7*24*time.Hour),

		// 设置日志切割时间间隔(1天)
		rotatelogs.WithRotationTime(24*time.Hour),
	)

	if err != nil {
		log.Fatalf("logrusInit add hook error：%v", err)
	}

	// 设置 rotatelogs
	error_file_writer, err := rotatelogs.New(
		// 分割后的文件名称
		logrus_error_file+".%Y%m%d.log",

		// 生成软链，指向最新日志文件
		//rotatelogs.WithLinkName(logrus_error_file),

		// 设置最大保存时间(7天)
		rotatelogs.WithMaxAge(7*24*time.Hour),

		// 设置日志切割时间间隔(1天)
		rotatelogs.WithRotationTime(24*time.Hour),
	)

	if err != nil {
		log.Fatalf("logrusInit add hook error：%v", err)
	}

	writeMap := lfshook.WriterMap{
		logrus.InfoLevel:  info_file_writer,
		logrus.FatalLevel: error_file_writer,
		logrus.DebugLevel: error_file_writer,
		logrus.WarnLevel:  error_file_writer,
		logrus.ErrorLevel: error_file_writer,
		logrus.PanicLevel: error_file_writer,
	}

	lfHook := lfshook.NewHook(writeMap, &logrus.JSONFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
	})

	// 新增 Hook
	logrus.AddHook(lfHook)

	// default
	logrus.SetReportCaller(true)
}
