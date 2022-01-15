package types

import "fmt"

type InitialPeerSearchMessage struct {
	RequestID  string
	Budget     uint
	FileID     string
	Originator string
}

type InitialPeerResponseMessage struct {
	RequestID string
	FileID    string
	NumParts  uint
}

type DataQueryRequest struct {
	FileID string
}

type DataQueryResponse struct {
	FileID          string
	AvailableBlocks [][2]uint
}

type DataDownloadRequest struct {
	FileID string
	PartID uint
}

type DataDownloadResponse struct {
	FileID string
	PartID uint
	Data   []byte
}

// NewEmpty implements types.Message.
func (d InitialPeerSearchMessage) NewEmpty() Message {
	return &InitialPeerSearchMessage{}
}

// Name implements types.Message.
func (d InitialPeerSearchMessage) Name() string {
	return "initialpeersearch"
}

// String implements types.Message.
func (d InitialPeerSearchMessage) String() string {
	return fmt.Sprintf("initialpeersearch{id:%s, budget:%d, file:%s, originator:%s}", d.RequestID, d.Budget, d.FileID, d.Originator)
}

// HTML implements types.Message.
func (d InitialPeerSearchMessage) HTML() string {
	return d.String()
}

// NewEmpty implements types.Message.
func (d InitialPeerResponseMessage) NewEmpty() Message {
	return &InitialPeerResponseMessage{}
}

// Name implements types.Message.
func (d InitialPeerResponseMessage) Name() string {
	return "initialpeerresponse"
}

// String implements types.Message.
func (d InitialPeerResponseMessage) String() string {
	return fmt.Sprintf("initialpeerresponse{id:%s}", d.RequestID)
}

// HTML implements types.Message.
func (d InitialPeerResponseMessage) HTML() string {
	return d.String()
}

// NewEmpty implements types.Message.
func (d DataQueryRequest) NewEmpty() Message {
	return &DataQueryRequest{}
}

// Name implements types.Message.
func (d DataQueryRequest) Name() string {
	return "dataqueryrequest"
}

// String implements types.Message.
func (d DataQueryRequest) String() string {
	return fmt.Sprintf("dataqueryrequest{file:%s}", d.FileID)
}

// HTML implements types.Message.
func (d DataQueryRequest) HTML() string {
	return d.String()
}

// NewEmpty implements types.Message.
func (d DataQueryResponse) NewEmpty() Message {
	return &DataQueryResponse{}
}

// Name implements types.Message.
func (d DataQueryResponse) Name() string {
	return "dataqueryresponse"
}

// String implements types.Message.
func (d DataQueryResponse) String() string {
	return fmt.Sprintf("dataqueryresponse{file:%s, available:%v}", d.FileID, d.AvailableBlocks)
}

// HTML implements types.Message.
func (d DataQueryResponse) HTML() string {
	return d.String()
}

// NewEmpty implements types.Message.
func (d DataDownloadRequest) NewEmpty() Message {
	return &DataDownloadRequest{}
}

// Name implements types.Message.
func (d DataDownloadRequest) Name() string {
	return "DataDownloadrequest"
}

// String implements types.Message.
func (d DataDownloadRequest) String() string {
	return fmt.Sprintf("DataDownloadrequest{file:%s, part:%d}", d.FileID, d.PartID)
}

// HTML implements types.Message.
func (d DataDownloadRequest) HTML() string {
	return d.String()
}

// NewEmpty implements types.Message.
func (d DataDownloadResponse) NewEmpty() Message {
	return &DataDownloadResponse{}
}

// Name implements types.Message.
func (d DataDownloadResponse) Name() string {
	return "DataDownloadresponse"
}

// String implements types.Message.
func (d DataDownloadResponse) String() string {
	return fmt.Sprintf("DataDownloadresponse{file:%s, part:%d, data:%v}", d.FileID, d.PartID, d.Data)
}

// HTML implements types.Message.
func (d DataDownloadResponse) HTML() string {
	return d.String()
}