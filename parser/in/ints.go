package in

type Ints struct {
	values []int
	index  map[int]bool
}

func (i *Ints) In(value int, values []int) bool {
	if len(i.values) == 0 {
		i.values = values
		i.index = make(map[int]bool)
		for _, v := range values {
			i.index[v] = true
		}
	}
	return i.index[value]
}

func (i *Ints) Set(values []int) {
	i.values = values
	i.index = make(map[int]bool)
	for _, v := range values {
		i.index[v] = true
	}
}

// NewInts returns a function that checks if a value is In a list of values.
func NewInts() *Ints {
	return &Ints{index: make(map[int]bool)}
}
