package efuse

import (
	"math"
	"time"
)

type PolyfuseData struct {
	Request   float64   `json:"request"`
	Error     float64   `json:"error"`
	Timestamp time.Time `json:"timestamp"`
}

type PolifuseStore interface {
	FetchData() (*PolyfuseData, error)
	PushData(PolyfuseData) error
	GetState(PolyfuseData) (bool, error)
}

type DefaultPolyfuseStore struct {
	data *PolyfuseData
}

func (s DefaultPolyfuseStore) FetchData() (*PolyfuseData, error) {
	return s.data, nil
}

func (s DefaultPolyfuseStore) PushData(d PolyfuseData) error {
	s.data.Request = d.Request
	s.data.Error = d.Error
	s.data.Timestamp = d.Timestamp
	return nil
}

func (s DefaultPolyfuseStore) GetState(PolyfuseData) (bool, error) { return false, nil }

// PolyfuseSettings setting for Polyfuse
type PolyfuseSettings struct {
	ID             string                                           // id of the fuse
	Rampframe      int                                              // rampup time, ignore error rate if happens recently, recommend 4s-10s, min 1s
	Timeframe      int                                              // counting data for give timeframe, in seconds
	MaxRequest     int                                              // max requests before tripping, <=0 means ulimited
	MaxError       int                                              // max error before tripping, <=0 means ulimited
	ErrorRate      int                                              // error rate before tripping, measuring in per 10000 (aka 100.00% precision), <=0 means ulimited
	MultiLock      bool                                             // specific safe update or not, not yet support
	UpdateDataFunc func(bool, *PolyfuseData) (*PolyfuseData, error) // data collect and stats should be here
}

type Polyfuse struct {
	reqPerSec float64
	errPerSec float64

	setting PolyfuseSettings // store settings
	store   PolifuseStore    // store data
}

// GetID return fuse ID
func (f *Polyfuse) GetID() string { return f.setting.ID }

// GetState return state of the fuse base on setting GetStateFunc(). If GetStateFunc() is not provided, the default function will be used
func (f *Polyfuse) GetState() (bool, error) {
	// get fuse data
	data, err := f.store.FetchData()
	if err != nil {
		return false, err
	}

	// snap data
	now := time.Now()
	distance := now.Sub(data.Timestamp)
	newReq := data.Request - (distance.Seconds() * f.reqPerSec)
	if newReq < 1 {
		newReq = 1
	}
	newErr := (data.Error - (distance.Seconds() * f.errPerSec))
	if newErr < 0 {
		newErr = 0
	}

	// check request limit
	if f.setting.MaxRequest > 0 && f.setting.MaxRequest <= int(newReq) {
		return false, nil
	}

	// check error limit
	if f.setting.MaxError > 0 && f.setting.MaxError <= int(newErr) {
		return false, nil
	}

	// check error rate
	if f.setting.ErrorRate > 0 && errorShift(newErr, newReq, float64(f.setting.ErrorRate)/10000, distance.Seconds(), float64(f.setting.Rampframe)) {
		return false, nil
	}

	return true, f.store.PushData(*data)
}

// PushState add a state data then push to storage
func (f *Polyfuse) PushState(state bool) error {
	// get fuse data
	data, err := f.store.FetchData()
	if err != nil {
		return err
	}

	// update fuse data
	data, err = f.setting.UpdateDataFunc(state, data)
	if err != nil {
		return err
	}

	// push fuse data to storage
	return f.store.PushData(*data)
}

func NewPolyfuse(setting PolyfuseSettings) EFuse {
	var f Polyfuse
	// init local data
	// f.data = PolyfuseData{Request: 1, Error: 0, Timestamp: time.Now()}

	// store
	f.store = DefaultPolyfuseStore{data: &PolyfuseData{Request: 1, Error: 0, Timestamp: time.Now()}}

	// precache rate setting
	f.reqPerSec = float64(setting.MaxRequest) / float64(setting.Timeframe)
	f.errPerSec = float64(setting.MaxError) / float64(setting.Timeframe)
	// set error rate limit
	if setting.ErrorRate < 0 {
		setting.ErrorRate = 0
	}
	if setting.ErrorRate > 10000 {
		setting.ErrorRate = 10000
	}
	if setting.Rampframe < 1 {
		setting.Rampframe = 1
	}

	if setting.UpdateDataFunc == nil {
		setting.UpdateDataFunc = defaultPolyfuseUpdateData(&f)
	}

	return &f
}

// defaultPolyfuseUpdateData default update data, stat equation should be put here
func defaultPolyfuseUpdateData(f *Polyfuse) func(bool, *PolyfuseData) (*PolyfuseData, error) {
	return func(state bool, data *PolyfuseData) (*PolyfuseData, error) {
		// snap data
		now := time.Now()
		distance := now.Sub(data.Timestamp)
		newReq := data.Request - (distance.Seconds() * f.reqPerSec)
		if newReq < 0 {
			newReq = 0
		}
		newReq += 1
		newErr := (data.Error - (distance.Seconds() * f.errPerSec))
		if newErr < 0 {
			newErr = 0
		}
		if !state {
			newErr += 1
		}

		// update fuse data
		data.Error = newErr
		data.Request = newReq
		data.Timestamp = now

		return data, nil
	}
}

// errorShift shifting error value to appropriate scale an compare
func errorShift(err, req, errRate, span, ramp float64) bool {
	currentRate := err / req
	sigmoidRate := 1 / (1 + math.Pow(math.E, (span*12/ramp)-6))
	normaliseRate := sigmoidRate*(1-errRate) + errRate
	return currentRate > normaliseRate
}
