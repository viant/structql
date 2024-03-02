package in

type Strings struct {
	values []string
	index  map[string]bool
}

func (s *Strings) In(value string, values []string) bool {
	if len(s.values) == 0 {
		s.values = values
		s.index = make(map[string]bool)
		for _, v := range values {
			s.index[v] = true
		}
	}
	return s.index[value]
}

func (s *Strings) Set(values []string) {
	s.values = values
	s.index = make(map[string]bool)
	for _, v := range values {
		s.index[v] = true
	}
}

// NewStrings returns a function that checks if a string is In a list of strings.
func NewStrings() *Strings {
	return &Strings{index: make(map[string]bool)}
}
