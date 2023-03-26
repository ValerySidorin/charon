package fiasnalog

type DownloadFileInfo struct {
	VersionID      int    `json:"versionId"`
	GARXMLDeltaURL string `json:"garXMLDeltaUrl"`
	GARXMLFullURL  string `json:"garXMLFullUrl"`
}
