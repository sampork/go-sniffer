// Copyright 2021 FerretDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"errors"
	"fmt"
	"math"

	"github.com/40t/go-sniffer/plugSrc/mongodb/build/FerretDB-internal/handler/handlererrors"
	"github.com/40t/go-sniffer/plugSrc/mongodb/build/FerretDB-internal/handler/handlerparams"
	"github.com/40t/go-sniffer/plugSrc/mongodb/build/FerretDB-internal/types"
	"github.com/40t/go-sniffer/plugSrc/mongodb/build/FerretDB-internal/util/lazyerrors"
	"github.com/40t/go-sniffer/plugSrc/mongodb/build/FerretDB-internal/util/must"
)

// GetRequiredParam returns doc's value for key
// or protocol error for missing key or invalid type.
func GetRequiredParam[T types.Type](doc *types.Document, key string) (T, error) {
	var zero T

	v, _ := doc.Get(key)
	if v == nil {
		msg := fmt.Sprintf("required parameter %q is missing", key)
		return zero, handlererrors.NewCommandErrorMsgWithArgument(handlererrors.ErrBadValue, msg, key)
	}

	res, ok := v.(T)
	if !ok {
		msg := fmt.Sprintf("required parameter %q has type %T (expected %T)", key, v, zero)
		return zero, handlererrors.NewCommandErrorMsgWithArgument(handlererrors.ErrBadValue, msg, key)
	}

	return res, nil
}

// GetOptionalParam returns doc's value for key, default value for missing parameter,
// or protocol error for invalid type.
func GetOptionalParam[T types.Type](doc *types.Document, key string, defaultValue T) (T, error) {
	v, _ := doc.Get(key)
	if v == nil {
		return defaultValue, nil
	}

	// require exact type; do not threat nulls as default values in this helper
	res, ok := v.(T)
	if !ok {
		msg := fmt.Sprintf(
			`BSON field '%s' is the wrong type '%s', expected type '%s'`,
			key, handlerparams.AliasFromType(v), handlerparams.AliasFromType(defaultValue),
		)

		return defaultValue, handlererrors.NewCommandErrorMsgWithArgument(handlererrors.ErrTypeMismatch, msg, key)
	}

	return res, nil
}

// GetOptionalNullParam returns doc's value for key, default value for missing parameter or null,
// or protocol error for other invalid type.
func GetOptionalNullParam[T types.Type](doc *types.Document, key string, defaultValue T) (T, error) {
	v, err := GetOptionalParam(doc, key, defaultValue)
	if err != nil {
		// the only possible error here is type mismatch, so the key is present
		if _, ok := must.NotFail(doc.Get(key)).(types.NullType); ok {
			err = nil
		}
	}

	return v, err
}

// AssertType asserts value's type, returning protocol error for unexpected types.
//
// If a custom error is needed, use a normal Go type assertion instead:
//
//	d, ok := value.(*types.Document)
//	if !ok {
//	  return handlererrors.NewCommandErrorMsg(handlererrors.ErrBadValue, "expected document")
//	}
func AssertType[T types.Type](value any) (T, error) {
	res, ok := value.(T)
	if !ok {
		msg := fmt.Sprintf("got type %T, expected %T", value, res)
		return res, handlererrors.NewCommandErrorMsg(handlererrors.ErrBadValue, msg)
	}

	return res, nil
}

// GetLimitParam returns limit value from provided query document.
func GetLimitParam(doc *types.Document) (int64, error) {
	v, _ := doc.Get("limit")
	if v == nil {
		return 0, nil
	}

	res, err := handlerparams.GetWholeNumberParam(v)
	if err != nil {
		msg := fmt.Sprintf(
			`BSON field '%s' is the wrong type '%s', expected type '%s'`,
			"limit", handlerparams.AliasFromType(v), handlerparams.AliasFromType(res),
		)

		return res, handlererrors.NewCommandErrorMsgWithArgument(handlererrors.ErrTypeMismatch, msg, "limit")
	}

	return res, nil
}

// GetLimitStageParam returns $limit stage argument from the provided value.
// It returns the proper error if value doesn't meet requirements.
func GetLimitStageParam(value any) (int64, error) {
	limit, err := handlerparams.GetWholeNumberParam(value)

	switch {
	case err == nil:
	case errors.Is(err, handlerparams.ErrUnexpectedType):
		return 0, handlererrors.NewCommandErrorMsgWithArgument(
			handlererrors.ErrStageLimitInvalidArg,
			fmt.Sprintf("invalid argument to $limit stage: Expected a number in: $limit: %#v", value),
			"$limit (stage)",
		)
	case errors.Is(err, handlerparams.ErrNotWholeNumber), errors.Is(err, handlerparams.ErrInfinity):
		return 0, handlererrors.NewCommandErrorMsgWithArgument(
			handlererrors.ErrStageLimitInvalidArg,
			fmt.Sprintf("invalid argument to $limit stage: Expected an integer: $limit: %#v", value),
			"$limit (stage)",
		)
	case errors.Is(err, handlerparams.ErrLongExceededPositive), errors.Is(err, handlerparams.ErrLongExceededNegative):
		return 0, handlererrors.NewCommandErrorMsgWithArgument(
			handlererrors.ErrStageLimitInvalidArg,
			fmt.Sprintf("invalid argument to $limit stage: Cannot represent as a 64-bit integer: $limit: %#v", value),
			"$limit (stage)",
		)
	default:
		return 0, lazyerrors.Error(err)
	}

	switch {
	case limit < 0:
		return 0, handlererrors.NewCommandErrorMsgWithArgument(
			handlererrors.ErrStageLimitInvalidArg,
			fmt.Sprintf("invalid argument to $limit stage: Expected a non-negative number in: $limit: %#v", limit),
			"$limit (stage)",
		)
	case limit == 0:
		return 0, handlererrors.NewCommandErrorMsgWithArgument(
			handlererrors.ErrStageLimitZero,
			"The limit must be positive",
			"$limit (stage)",
		)
	}

	return limit, nil
}

// GetSkipStageParam returns $skip stage argument from the provided value.
// It returns the proper error if value doesn't meet requirements.
func GetSkipStageParam(value any) (int64, error) {
	limit, err := handlerparams.GetWholeNumberParam(value)

	switch {
	case err == nil:
	case errors.Is(err, handlerparams.ErrNotWholeNumber),
		errors.Is(err, handlerparams.ErrInfinity),
		errors.Is(err, handlerparams.ErrUnexpectedType):
		return 0, handlererrors.NewCommandErrorMsgWithArgument(
			handlererrors.ErrStageSkipBadValue,
			fmt.Sprintf("invalid argument to $skip stage: Expected an integer: $skip: %#v", value),
			"$skip (stage)",
		)
	case errors.Is(err, handlerparams.ErrLongExceededPositive), errors.Is(err, handlerparams.ErrLongExceededNegative):
		return 0, handlererrors.NewCommandErrorMsgWithArgument(
			handlererrors.ErrStageSkipBadValue,
			fmt.Sprintf("invalid argument to $skip stage: Cannot represent as a 64-bit integer: $skip: %#v", value),
			"$skip (stage)",
		)
	default:
		return 0, lazyerrors.Error(err)
	}

	switch {
	case limit < 0:
		return 0, handlererrors.NewCommandErrorMsgWithArgument(
			handlererrors.ErrStageSkipBadValue,
			fmt.Sprintf("invalid argument to $skip stage: Expected a non-negative number in: $skip: %#v", limit),
			"$skip (stage)",
		)
	}

	return limit, nil
}

// getBinaryMaskParam matches value type, returning bit mask and error if match failed.
// Possible values are: position array ([1,3,5] == 010101), whole number value and types.Binary value.
func getBinaryMaskParam(operator string, mask any) (uint64, error) {
	var bitmask uint64

	switch mask := mask.(type) {
	case *types.Array:
		// {field: {$bitsAllClear: [position1, position2]}}
		for i := 0; i < mask.Len(); i++ {
			val := must.NotFail(mask.Get(i))

			b, err := handlerparams.GetWholeNumberParam(val)
			if err != nil {
				switch {
				case errors.Is(err, handlerparams.ErrNotWholeNumber), errors.Is(err, handlerparams.ErrInfinity),
					errors.Is(err, handlerparams.ErrLongExceededPositive), errors.Is(err, handlerparams.ErrLongExceededNegative):
					return 0, handlererrors.NewCommandErrorMsgWithArgument(
						handlererrors.ErrBadValue,
						fmt.Sprintf(`Failed to parse bit position. Expected an integer: %d: %#v`, i, val),
						operator,
					)
				default:
					return 0, handlererrors.NewCommandErrorMsgWithArgument(
						handlererrors.ErrBadValue,
						fmt.Sprintf(`Failed to parse bit position. Expected a number in: %d: %#v`, i, val),
						operator,
					)
				}
			}

			if b < 0 {
				return 0, handlererrors.NewCommandErrorMsgWithArgument(
					handlererrors.ErrBadValue,
					fmt.Sprintf("Failed to parse bit position. Expected a non-negative number in: %d: %d", i, b),
					operator,
				)
			}

			bitmask |= 1 << uint64(math.Min(float64(b), 63))
		}

	case float64:
		// {field: {$bitsAllClear: bitmask}}
		if mask != math.Trunc(mask) || math.IsInf(mask, 0) {
			return 0, handlererrors.NewCommandErrorMsgWithArgument(
				handlererrors.ErrFailedToParse,
				fmt.Sprintf("Expected an integer: %s: %#v", operator, mask),
				operator,
			)
		}

		if mask < 0 {
			return 0, handlererrors.NewCommandErrorMsgWithArgument(
				handlererrors.ErrFailedToParse,
				fmt.Sprintf(`Expected a non-negative number in: %s: %.1f`, operator, mask),
				operator,
			)
		}

		bitmask = uint64(mask)

	case types.Binary:
		// {field: {$bitsAllClear: BinData()}}
		for b := 0; b < len(mask.B); b++ {
			byteAt := mask.B[b]

			if byteAt == 0 {
				continue
			}

			if b < 8 {
				bitmask |= uint64(byteAt) << uint64(b*8)
			} else {
				bitmask |= 1 << 63
			}
		}

	case int32:
		// {field: {$bitsAllClear: bitmask}}
		if mask < 0 {
			return 0, handlererrors.NewCommandErrorMsgWithArgument(
				handlererrors.ErrFailedToParse,
				fmt.Sprintf(`Expected a non-negative number in: %s: %v`, operator, mask),
				operator,
			)
		}

		bitmask = uint64(mask)

	case int64:
		// {field: {$bitsAllClear: bitmask}}
		if mask < 0 {
			return 0, handlererrors.NewCommandErrorMsgWithArgument(
				handlererrors.ErrFailedToParse,
				fmt.Sprintf(`Expected a non-negative number in: %s: %v`, operator, mask),
				operator,
			)
		}

		bitmask = uint64(mask)

	default:
		return 0, handlererrors.NewCommandErrorMsgWithArgument(
			handlererrors.ErrBadValue,
			fmt.Sprintf(`value takes an Array, a number, or a BinData but received: %s: %#v`, operator, mask),
			operator,
		)
	}

	return bitmask, nil
}

// addNumbers returns the result of v1 and v2 addition and error if addition failed.
// The v1 and v2 parameters could be float64, int32, int64.
// The result would be the broader type possible, i.e. int32 + int64 produces int64.
func addNumbers(v1, v2 any) (any, error) {
	switch v1 := v1.(type) {
	case float64:
		switch v2 := v2.(type) {
		case float64:
			return v1 + v2, nil
		case int32:
			return v1 + float64(v2), nil
		case int64:
			return v1 + float64(v2), nil
		default:
			return nil, handlerparams.ErrUnexpectedRightOpType
		}
	case int32:
		switch v2 := v2.(type) {
		case float64:
			return v2 + float64(v1), nil
		case int32:
			if v2 == math.MaxInt32 && v1 > 0 {
				return int64(v1) + int64(v2), nil
			}

			if v2 == math.MinInt32 && v1 < 0 {
				return int64(v1) + int64(v2), nil
			}

			return v1 + v2, nil
		case int64:
			if v2 > 0 {
				if int64(v1) > math.MaxInt64-v2 {
					return nil, handlerparams.ErrLongExceededPositive
				}
			} else {
				if int64(v1) < math.MinInt64-v2 {
					return nil, handlerparams.ErrLongExceededNegative
				}
			}

			return v2 + int64(v1), nil
		default:
			return nil, handlerparams.ErrUnexpectedRightOpType
		}
	case int64:
		switch v2 := v2.(type) {
		case float64:
			return v2 + float64(v1), nil
		case int32:
			if v2 > 0 {
				if v1 > math.MaxInt64-int64(v2) {
					return nil, handlerparams.ErrIntExceeded
				}
			} else {
				if v1 < math.MinInt64-int64(v2) {
					return nil, handlerparams.ErrIntExceeded
				}
			}

			return v1 + int64(v2), nil
		case int64:
			if v2 > 0 {
				if v1 > math.MaxInt64-v2 {
					return nil, handlerparams.ErrLongExceededPositive
				}
			} else {
				if v1 < math.MinInt64-v2 {
					return nil, handlerparams.ErrLongExceededNegative
				}
			}

			return v1 + v2, nil
		default:
			return nil, handlerparams.ErrUnexpectedRightOpType
		}
	default:
		return nil, handlerparams.ErrUnexpectedLeftOpType
	}
}

// multiplyNumbers returns the multiplication of v1 and v2.
// The v1 and v2 parameters could be float64, int32 and int64.
// Multiplication of negative number with zero produces 0, not -0.
// The produced result maybe be the broader type:
// int32 * int64 produces int64.
func multiplyNumbers(v1, v2 any) (any, error) {
	switch v1 := v1.(type) {
	case float64:
		var res float64

		switch v2 := v2.(type) {
		case float64:
			res = v1 * v2
		case int32:
			res = v1 * float64(v2)
		case int64:
			res = v1 * float64(v2)
		default:
			return nil, handlerparams.ErrUnexpectedRightOpType
		}

		return res, nil
	case int32:
		switch v2 := v2.(type) {
		case float64:
			return float64(v1) * v2, nil
		case int32:
			res := v1 * v2
			if int64(res) != int64(v1)*int64(v2) {
				return int64(v1) * int64(v2), nil
			}

			return res, nil
		case int64:
			return multiplyLongSafely(int64(v1), v2)

		default:
			return nil, handlerparams.ErrUnexpectedRightOpType
		}
	case int64:
		switch v2 := v2.(type) {
		case float64:
			return float64(v1) * v2, nil
		case int32:
			v, err := multiplyLongSafely(v1, int64(v2))
			if err != nil {
				return 0, handlerparams.ErrIntExceeded
			}

			return v, nil
		case int64:
			return multiplyLongSafely(v1, v2)

		default:
			return nil, handlerparams.ErrUnexpectedRightOpType
		}
	default:
		return nil, handlerparams.ErrUnexpectedLeftOpType
	}
}

// multiplyLongSafely returns the multiplication of two int64 values.
// It handles int64 overflows, and returns errLongExceeded error on one.
//
// Please always use multiplyNumbers as it calls multiplyLongSafely when needed.
func multiplyLongSafely(v1, v2 int64) (int64, error) {
	switch {
	// 0 and 1 values are excluded, because those are only values that
	// can multiply `MinInt64` without exceeding the range.
	case v1 == 0 || v2 == 0 || v1 == 1 || v2 == 1:
		return v1 * v2, nil

	// Multiplying MinInt64 by any other value than above results in overflow.
	// This check is necessary only for MinInt64, as multiplying MinInt64 by -1
	// results in overflow with the MinInt64 as result.
	case v1 == math.MinInt64 || v2 == math.MinInt64:
		return 0, handlerparams.ErrLongExceededNegative
	}

	res := v1 * v2
	if res/v2 != v1 {
		return 0, handlerparams.ErrLongExceededPositive
	}

	return res, nil
}

// performBitLogic returns the result of a bitwise operation on two Integer(int32/int64) values.
// It only supports 'and', 'or' and 'xor' bitwise operators.
//
// The result produced will be the broader type: int32 & int64 = int64.
func performBitLogic(bitOp string, v1, v2 any) (any, error) {
	switch bitOp {
	case "and":
		switch v1 := v1.(type) {
		case int32:
			switch v2 := v2.(type) {
			case int32:
				return v1 & v2, nil
			case int64:
				return int64(v1) & v2, nil
			default:
				return nil, handlerparams.ErrUnexpectedRightOpType
			}

		case int64:
			switch v2 := v2.(type) {
			case int32:
				return v1 & int64(v2), nil
			case int64:
				return v1 & v2, nil
			default:
				return nil, handlerparams.ErrUnexpectedRightOpType
			}

		default:
			return nil, handlerparams.ErrUnexpectedLeftOpType
		}

	case "or":
		switch v1 := v1.(type) {
		case int32:
			switch v2 := v2.(type) {
			case int32:
				return v1 | v2, nil
			case int64:
				return int64(v1) | v2, nil
			default:
				return nil, handlerparams.ErrUnexpectedRightOpType
			}

		case int64:
			switch v2 := v2.(type) {
			case int32:
				return v1 | int64(v2), nil

			case int64:
				return v1 | v2, nil
			default:
				return nil, handlerparams.ErrUnexpectedRightOpType
			}

		default:
			return nil, handlerparams.ErrUnexpectedLeftOpType
		}

	case "xor":
		switch v1 := v1.(type) {
		case int32:
			switch v2 := v2.(type) {
			case int32:
				return v1 ^ v2, nil
			case int64:
				return int64(v1) ^ v2, nil
			default:
				return nil, handlerparams.ErrUnexpectedRightOpType
			}

		case int64:
			switch v2 := v2.(type) {
			case int32:
				return v1 ^ int64(v2), nil
			case int64:
				return v1 ^ v2, nil
			default:
				return nil, handlerparams.ErrUnexpectedRightOpType
			}

		default:
			return nil, handlerparams.ErrUnexpectedLeftOpType
		}

	default:
		return nil, fmt.Errorf(
			`The $bit modifier only supports 'and', 'or', and 'xor', not '%s'`+
				` which is an unknown operator: {%s: %v}`,
			bitOp,
			bitOp,
			types.FormatAnyValue(v1),
		)
	}
}
