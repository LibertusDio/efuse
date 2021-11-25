package efuse

import (
	"sync"
	"time"
)

type PolyfuseData struct {
	Request   float64   `json:"request"`
	Error     float64   `json:"error"`
	Timestamp time.Time `json:"timestamp"`
}

// PolyfuseSettings setting for Polyfuse
type PolyfuseSettings struct {
	ID             string                                         // id of the fuse
	Timeframe      int                                            // counting data for give timeframe, in seconds
	MaxRequest     int                                            // max requests before tripping, <=0 means ulimited
	MaxError       int                                            // max error before tripping, <=0 means ulimited
	ErrorRate      int                                            // error rate before tripping, measuring in per 10000 (aka 100.00% precision), <=0 means ulimited
	MultiLock      bool                                           // specific safe update or not, not yet support
	FetchDataFunc  func() (PolyfuseData, error)                   // should be provided if multi-instance
	PushDataFunc   func(PolyfuseData) error                       // should be provided if multi-instance
	GetStateFunc   func(PolyfuseData) (bool, error)               // decision making function
	UpdateDataFunc func(bool, PolyfuseData) (PolyfuseData, error) // data collect and stats should be here
}

type Polyfuse struct {
	mutex     sync.RWMutex
	reqPerSec float64
	errPerSec float64

	data    PolyfuseData     // store data locally
	setting PolyfuseSettings // store settings
}

// GetID return fuse ID
func (f *Polyfuse) GetID() string { return f.setting.ID }

// GetState return state of the fuse base on setting GetStateFunc(). If GetStateFunc() is not provided, the default function will be used
func (f *Polyfuse) GetState() (bool, error) {
	// get fuse data
	data, err := f.setting.FetchDataFunc()
	if err != nil {
		return false, err
	}

	// return state based on GetStateFunc()
	return f.setting.GetStateFunc(data)
}

// PushState add a state data then push to storage
func (f *Polyfuse) PushState(state bool) error {
	// get fuse data
	data, err := f.setting.FetchDataFunc()
	if err != nil {
		return err
	}

	// update fuse data
	data, err = f.setting.UpdateDataFunc(state, data)
	if err != nil {
		return err
	}

	// push fuse data to storage
	return f.setting.PushDataFunc(data)
}

func NewPolyfuse(setting PolyfuseSettings) EFuse {
	var f Polyfuse
	// init local data
	f.data = PolyfuseData{Request: 0, Error: 0, Timestamp: time.Now()}
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

	// using default function if not provided
	if setting.FetchDataFunc == nil || setting.PushDataFunc == nil {
		setting.FetchDataFunc = defaultPolyfuseFetchData(&f)
		setting.PushDataFunc = defaultPolyfusePushData(&f)
	}
	if setting.GetStateFunc == nil {
		setting.GetStateFunc = defaultPolyfuseGetState(&f)
	}
	if setting.UpdateDataFunc == nil {
		setting.UpdateDataFunc = defaultPolyfuseUpdateData(&f)
	}

	return &f
}

// defaultPolyfuseFetchData default fetch data for polyfuse
func defaultPolyfuseFetchData(f *Polyfuse) func() (PolyfuseData, error) {
	return func() (PolyfuseData, error) {
		return f.data, nil
	}
}

// defaultPolyfusePushData default update data for polyfuse
func defaultPolyfusePushData(f *Polyfuse) func(PolyfuseData) error {
	return func(data PolyfuseData) error {
		f.data = data
		return nil
	}
}

// defaultPolyfuseGetState default get state from data for polyfuse, decision making opinion should be here
func defaultPolyfuseGetState(f *Polyfuse) func(PolyfuseData) (bool, error) {
	return func(data PolyfuseData) (bool, error) {
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
		if f.setting.ErrorRate > 0 && f.setting.ErrorRate <= int((newErr/newReq)*10000) {
			return false, nil
		}

		// update fuse data
		data.Error = newErr
		data.Request = newReq
		data.Timestamp = now

		return true, f.setting.PushDataFunc(data)
	}
}

// defaultPolyfuseUpdateData default update data, stat equation should be put here
func defaultPolyfuseUpdateData(f *Polyfuse) func(bool, PolyfuseData) (PolyfuseData, error) {
	return func(state bool, data PolyfuseData) (PolyfuseData, error) {
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
