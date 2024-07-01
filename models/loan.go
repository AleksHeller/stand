package models

import (
	"math/rand"
	"strconv"
	"time"
)

var letterBytes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randomString(length int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	str := make([]byte, length)
	for i := range str {
		str[i] = letterBytes[r.Intn(len(letterBytes))]
	}
	return string(str)
}

func randomAmount() float32 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return float32((r.Intn(16) + 15) * 1000)
}

func randomPeriod() int {
	period := [2]int{31, 60}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return period[r.Intn(2)]
}

func randomLeadId() int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Intn(99999) + 600001
}

func randomCurrentStatus() string {
	currentStatus := [6]string{"pending", "rejected", "approved", "active", "close", "to issue"}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return currentStatus[r.Intn(6)]
}

func randomInt() int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Intn(9999) + 1000001
}

func randomFloat32() float32 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Float32()
}

type Loan struct {
	LastName                string  `json:"lastName"`
	FirstName               string  `json:"firstName"`
	Patronymic              string  `json:"patronymic"`
	Amount                  float32 `json:"amount"`
	Period                  int     `json:"period"`
	LeadId                  int     `json:"leadId"`
	CurrentStatus           string  `json:"currentStatus"`
	UrlContract             string  `json:"urlContract"`
	CreationDate            string  `json:"creationDate"`
	DateIssue               string  `json:"dateIssue"`
	ContractName            string  `json:"contractName"`
	InterestForTranche      float32 `json:"interestForTranche"`
	LoanDeniedRejectionId   int     `json:"loanDeniedRejectionId"`
	LoanDeniedRejectionText string  `json:"loanDeniedRejectionText"`
}

func New() *Loan {
	return &Loan{
		LastName:                randomString(6),
		FirstName:               randomString(4),
		Patronymic:              randomString(8),
		Amount:                  randomAmount(),
		Period:                  randomPeriod(),
		LeadId:                  randomLeadId(),
		CurrentStatus:           randomCurrentStatus(),
		UrlContract:             randomString(43),
		CreationDate:            randomString(10),
		DateIssue:               randomString(10),
		ContractName:            strconv.Itoa(randomInt()),
		InterestForTranche:      randomFloat32(),
		LoanDeniedRejectionId:   randomInt(),
		LoanDeniedRejectionText: randomString(22),
	}
}
