package common

import (
    "gotest.tools/assert"
    "testing"
    "time"
)

func TestRegularTimer(t *testing.T) {
    timer := NewTimer()
    before := timer.NanoTimeNow()
    timer.Sleep(100 * time.Millisecond)
    after := timer.NanoTimeNow()

    if after-before < 100*time.Millisecond.Nanoseconds() {
        t.Errorf("Sleep for RegularTimer doesn't work properly")
    }

    newTime := after + time.Hour.Nanoseconds()

    // This should not take any effect
    timer.SetNanoTimeNow(newTime)
    if timer.NanoTimeNow()-after > time.Second.Nanoseconds() {
        t.Errorf("SetNanoTime for regular timer should not take effect")
    }
}

func TestMockTimer(t *testing.T) {
    timer := NewMockTimer()
    assert.Equal(t, int64(0), timer.NanoTimeNow())

    timer.Sleep(100 * time.Millisecond)
    assert.Equal(t, 100*time.Millisecond.Nanoseconds(), timer.NanoTimeNow())

    timer.Sleep(100 * time.Millisecond)
    assert.Equal(t, 200*time.Millisecond.Nanoseconds(), timer.NanoTimeNow())

    timer.SetNanoTimeNow(500 * time.Millisecond.Nanoseconds())
    assert.Equal(t, 500*time.Millisecond.Nanoseconds(), timer.NanoTimeNow())

    timer.Sleep(100 * time.Millisecond)
    assert.Equal(t, 600*time.Millisecond.Nanoseconds(), timer.NanoTimeNow())
}
