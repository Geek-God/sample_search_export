// Package elasticModel
// @author:WXZ
// @date:2022/12/1
// @note

package elasticModel


// Samples
// @Author WXZ
// @Description: //TODO
type Samples struct {
	Model
	Addtime                    string `json:"addtime"`
	AvpScanName                string `json:"avp_scan_name"`
	AvpScanVirusName           string `json:"avp_scan_virus_name"`
	AvpScanVirusPlatform       string `json:"avp_scan_virus_platform"`
	AvpScanVirusTech           string `json:"avp_scan_virus_tech"`
	AvpScanVirusType           string `json:"avp_scan_virus_type"`
	Die                        string `json:"die"`
	EmuScanID                  string `json:"emu_scan_id"`
	EmuScanName                string `json:"emu_scan_name"`
	EmuScanVirusName           string `json:"emu_scan_virus_name"`
	EmuScanVirusPlatform       string `json:"emu_scan_virus_platform"`
	EmuScanVirusTech           string `json:"emu_scan_virus_tech"`
	EmuScanVirusType           string `json:"emu_scan_virus_type"`
	EsetScanName               string `json:"eset_scan_name"`
	EsetScanVirusName          string `json:"eset_scan_virus_name"`
	EsetScanVirusPlatform      string `json:"eset_scan_virus_platform"`
	EsetScanVirusTech          string `json:"eset_scan_virus_tech"`
	EsetScanVirusType          string `json:"eset_scan_virus_type"`
	FdfsPath                   string `json:"fdfs_path"`
	Filesize                   int64  `json:"filesize"`
	Filetype                   string `json:"filetype"`
	Hashsig                    string `json:"hashsig"`
	HrDefinedScanID            string `json:"hr_defined_scan_id"`
	HrDefinedScanName          string `json:"hr_defined_scan_name"`
	HrDefinedScanVirusName     string `json:"hr_defined_scan_virus_name"`
	HrDefinedScanVirusPlatform string `json:"hr_defined_scan_virus_platform"`
	HrDefinedScanVirusTech     string `json:"hr_defined_scan_virus_tech"`
	HrDefinedScanVirusType     string `json:"hr_defined_scan_virus_type"`
	HrScanID                   string `json:"hr_scan_id"`
	HrScanName                 string `json:"hr_scan_name"`
	HrScanVirusName            string `json:"hr_scan_virus_name"`
	HrScanVirusPlatform        string `json:"hr_scan_virus_platform"`
	HrScanVirusTech            string `json:"hr_scan_virus_tech"`
	HrScanVirusType            string `json:"hr_scan_virus_type"`
	HrTestScanID               string `json:"hr_test_scan_id"`
	HrTestScanName             string `json:"hr_test_scan_name"`
	HrTestScanVirusName        string `json:"hr_test_scan_virus_name"`
	HrTestScanVirusPlatform    string `json:"hr_test_scan_virus_platform"`
	HrTestScanVirusTech        string `json:"hr_test_scan_virus_tech"`
	HrTestScanVirusType        string `json:"hr_test_scan_virus_type"`
	//LastAddtime                string `json:"last_addtime"`
	Md5                  string   `json:"md5"`
	Modtime              string   `json:"modtime"`
	MsScanName           string   `json:"ms_scan_name"`
	MsScanVirusName      string   `json:"ms_scan_virus_name"`
	MsScanVirusPlatform  string   `json:"ms_scan_virus_platform"`
	MsScanVirusTech      string   `json:"ms_scan_virus_tech"`
	MsScanVirusType      string   `json:"ms_scan_virus_type"`
	QvmScanName          string   `json:"qvm_scan_name"`
	QvmScanVirusName     string   `json:"qvm_scan_virus_name"`
	QvmScanVirusPlatform string   `json:"qvm_scan_virus_platform"`
	QvmScanVirusTech     string   `json:"qvm_scan_virus_tech"`
	QvmScanVirusType     string   `json:"qvm_scan_virus_type"`
	RelScanID            string   `json:"rel_scan_id"`
	RelScanName          string   `json:"rel_scan_name"`
	RelScanVirusName     string   `json:"rel_scan_virus_name"`
	RelScanVirusPlatform string   `json:"rel_scan_virus_platform"`
	RelScanVirusTech     string   `json:"rel_scan_virus_tech"`
	RelScanVirusType     string   `json:"rel_scan_virus_type"`
	Sha1                 string   `json:"sha1"`
	Sha256               string   `json:"sha256"`
	Sha512               string   `json:"sha512"`
	Simhash              string   `json:"simhash"`
	Srclist              []string `json:"srclist"`
	Tags                 []string `json:"tags"`
}

func (s Samples) GetIndex() string {
	return "samples2"
}
func (s Samples) GetType() string {
	return "sampinfos"
}
