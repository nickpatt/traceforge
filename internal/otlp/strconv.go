package otlp

import "strconv"

func itoa(i int64) string   { return strconv.FormatInt(i, 10) }
func ftoa(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }
