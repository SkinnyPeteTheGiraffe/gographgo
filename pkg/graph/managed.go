package graph

import "time"

type IsLastStepManager struct{}

func (IsLastStepManager) Get(scratchpad *PregelScratchpad) any {
	return scratchpad.Step == scratchpad.Stop-1
}

type RemainingStepsManager struct{}

func (RemainingStepsManager) Get(scratchpad *PregelScratchpad) any {
	return scratchpad.Stop - scratchpad.Step
}

type CurrentUnixTimestampManager struct{}

func (CurrentUnixTimestampManager) Get(_ *PregelScratchpad) any {
	return time.Now().Unix()
}
