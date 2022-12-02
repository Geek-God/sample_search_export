// Package minioService
// @author:WXZ
// @date:2022/2/23
// @note

package minioService

import (
	"errors"
	"fmt"
	"sample_search_export/initd/minioInitd"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-go"
)

// FileObject
// @Author WXZ
// @Description: //TODO 上传文件的基础信息
type FileObject struct {
	Name     string
	FilePath string
	Opts     *minio.PutObjectOptions
}

var (
	bucketNamePrefix = "sample-"
)

// Upload
// @Author WXZ
// @Description: //TODO 上传文件
// @param objectPath string 上传对象路径
// @return error
func Upload(objectPath string) error {
	//获取文件信息
	file_info, err := os.Stat(objectPath)
	if err != nil {
		return err
	}

	if file_info.IsDir() {
		return errors.New("路径不能为文件夹")
	}

	var (
		bucket_name      string
		upload_file_name string = file_info.Name()
	)

	if ok, _ := filepath.Match("*.zip", upload_file_name); !ok {
		return errors.New("上传文件格式错误 file name: " + upload_file_name)
	}

	//获取桶名称
	if index := strings.Index(objectPath, "/dmp/"); index > 0 {
		//dmp需要特殊处理
		bucket_name = bucketNamePrefix + "dmp"
	} else if index := strings.Index(objectPath, "/P0/"); index > 0 {
		bucket_name = bucketNamePrefix + "p"
		engine_type := filepath.Base(filepath.Dir(objectPath))

		file_data_name := strings.Trim(upload_file_name, ".zip")
		t, err := time.ParseInLocation("2006010215", file_data_name, time.Local)
		if err != nil {
			return err
		}

		//P0预扫队列需要多级目录上传
		upload_file_name = fmt.Sprintf("%s/%s/%s", engine_type, t.Format("2006/01"), upload_file_name)
	} else {
		bucket_name, err = GetBucketName(upload_file_name)
		if err != nil {
			return err
		}
	}

	object := FileObject{
		upload_file_name,
		objectPath,
		&minio.PutObjectOptions{ContentType: "application/zip"},
	}
	//上传文件
	_, err = UploadObject(bucket_name, object)

	return err
}

// UploadObject
// @Author WXZ
// @Description: //TODO minio上传文件
// @param bucketName string 上传桶名称
// @param object Object 上传文件对象属性
// @return int64
// @return error
func UploadObject(bucketName string, object FileObject) (int64, error) {
	ok, err := minioInitd.Client().BucketExists(bucketName)
	if err != nil {
		return 0, err
	}

	if !ok {
		err = minioInitd.Client().MakeBucket(bucketName, "cn-north-1")
		if err != nil {
			return 0, err
		}
	}

	n, err := minioInitd.Client().FPutObject(bucketName, object.Name, object.FilePath, *object.Opts)

	return n, err
}

// GetBucketName
// @Author WXZ
// @Description: //TODO 获取桶名称
// @param fileName string
// @return string
// @return error
func GetBucketName(fileName string) (string, error) {
	if fileName == "" {
		return "", errors.New("文件名不能为空")
	}

	suffix := strings.ToLower(fileName[:1])
	return bucketNamePrefix + suffix, nil
}
