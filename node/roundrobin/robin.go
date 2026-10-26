package roundrobin

type Client interface {
	Read(key string) (string, error)
	Create(key, value string) error
	Update(key, value string) error
	Delete(key string) error
}
