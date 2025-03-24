package splitmix64

const IncrementConstant = 0x9e3779b97f4a7c15

func next(x0 uint64) uint64 {
	x0 = (x0 ^ (x0 >> 30)) * 0xbf58476d1ce4e5b9
	x0 = (x0 ^ (x0 >> 27)) * 0x94d049bb133111eb
	return x0 ^ (x0 >> 31)
}

func Splitmix64(state *uint64) uint64 {
	*state += IncrementConstant
	return next(*state)
}
