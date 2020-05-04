package main

type stringList []string

func (i *stringList) String() string {
	return "my string representation"
}

func (i *stringList) Set(value string) error {
	*i = append(*i, value)
	return nil
}
