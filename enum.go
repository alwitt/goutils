package goutils

import (
	"reflect"
	"slices"

	"github.com/go-playground/validator/v10"
)

// Enum is the constraint satisfied by a string-backed ENUM type that can enumerate its own
// members via a Values() method.
//
// MCPInstallEnumSchema uses it to register an enumerated schema for the type without the
// caller having to spell out the member list.
type Enum[T ~string] interface {
	~string
	// Values returns the complete set of permitted members for the ENUM type.
	Values() []T
}

// ======================================================================================
// String ENUM Types
// ======================================================================================

// ContainerStopSignalENUMType signal used to request the container process to stop
type ContainerStopSignalENUMType string

const (
	// ContainerStopSignalSIGINT stop the container process with SIGINT
	ContainerStopSignalSIGINT ContainerStopSignalENUMType = "SIGINT"
	// ContainerStopSignalSIGTERM stop the container process with SIGTERM
	ContainerStopSignalSIGTERM ContainerStopSignalENUMType = "SIGTERM"
	// ContainerStopSignalSIGQUIT stop the container process with SIGQUIT
	ContainerStopSignalSIGQUIT ContainerStopSignalENUMType = "SIGQUIT"
	// ContainerStopSignalSIGHUP stop the container process with SIGHUP
	ContainerStopSignalSIGHUP ContainerStopSignalENUMType = "SIGHUP"
	// ContainerStopSignalSIGKILL forcibly stop the container process with SIGKILL
	ContainerStopSignalSIGKILL ContainerStopSignalENUMType = "SIGKILL"
)

// Values all valid ContainerStopSignalENUMType values
func (ContainerStopSignalENUMType) Values() []ContainerStopSignalENUMType {
	return []ContainerStopSignalENUMType{
		ContainerStopSignalSIGINT,
		ContainerStopSignalSIGTERM,
		ContainerStopSignalSIGQUIT,
		ContainerStopSignalSIGHUP,
		ContainerStopSignalSIGKILL,
	}
}

// ContainerTimeoutPolicyENUMType how a timeout is packaged into the result
type ContainerTimeoutPolicyENUMType string

const (
	// ContainerTimeoutPolicyError package a timeout as an error (IsError: true)
	ContainerTimeoutPolicyError ContainerTimeoutPolicyENUMType = "TIMEOUT_IS_ERROR"
	// ContainerTimeoutPolicyOK package a timeout as a successful result carrying whatever
	// partial output was captured (ExitCode reports the sentinel 124)
	ContainerTimeoutPolicyOK ContainerTimeoutPolicyENUMType = "TIMEOUT_IS_OK"
)

// Values all valid ContainerTimeoutPolicyENUMType values
func (ContainerTimeoutPolicyENUMType) Values() []ContainerTimeoutPolicyENUMType {
	return []ContainerTimeoutPolicyENUMType{
		ContainerTimeoutPolicyError,
		ContainerTimeoutPolicyOK,
	}
}

// ======================================================================================
// String ENUM Validation
// ======================================================================================

// ENUM validator tags registered with a validator by RegisterWithValidator.
const (
	// ValidatorTagContainerStopSignal validate tag for ContainerStopSignalENUMType
	ValidatorTagContainerStopSignal = "container_stop_signal"
	// ValidatorTagContainerTimeoutPolicy validate tag for ContainerTimeoutPolicyENUMType
	ValidatorTagContainerTimeoutPolicy = "container_timeout_policy"
)

/*
ValidateStringENUM builds a validator.Func that accepts a field iff its string value is one
of the ENUM's declared Values. The single source of truth is the ENUM's Values method, so
adding a member there keeps validation in sync.

	@returns the validation function
*/
func ValidateStringENUM[T Enum[T]]() validator.Func {
	return func(fl validator.FieldLevel) bool {
		if fl.Field().Kind() != reflect.String {
			return false
		}
		got := T(fl.Field().String())
		return slices.Contains(got.Values(), got)
	}
}

/*
RegisterENUMInValidator register a string ENUM's validation under the given tag.

	@param v *validator.Validate - the validator to register against
	@param tag string - the validate tag the ENUM is referenced by
	@param fn validator.Func - the validation function (e.g. from ValidateStringENUM)
	@returns whether successful
*/
func RegisterENUMInValidator(v *validator.Validate, tag string, fn validator.Func) error {
	return v.RegisterValidation(tag, fn)
}

/*
RegisterWithValidator register every string ENUM defined by this package with the validator
so their validate tags resolve.

	@param v *validator.Validate - the validator to register against
	@returns whether successful
*/
func RegisterWithValidator(v *validator.Validate) error {
	if err := RegisterENUMInValidator(
		v, ValidatorTagContainerStopSignal, ValidateStringENUM[ContainerStopSignalENUMType](),
	); err != nil {
		return err
	}
	if err := RegisterENUMInValidator(
		v, ValidatorTagContainerTimeoutPolicy, ValidateStringENUM[ContainerTimeoutPolicyENUMType](),
	); err != nil {
		return err
	}
	return nil
}
