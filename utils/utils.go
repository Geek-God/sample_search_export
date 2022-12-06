// Package utils
// @author:WXZ
// @date:2022/11/30
// @note

package utils

import (
	"os"
	"regexp"
)

// SampleSha1Metch
// @Author WXZ
// @Description: //TODO 匹配sha1
// @param str string
// @return bool
// @return error
func SampleSha1Metch(str string) (bool, error) {
	return regexp.MatchString(`[\w\d]{40}`, str)
}

// FileExists
// @Author WXZ
// @Description: //TODO
// @param path string
// @return bool
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
