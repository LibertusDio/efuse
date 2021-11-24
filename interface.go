package efuse

type EFuse interface {
	GetID() string
	GetState() (bool, error)
	PushState(bool) error
}
