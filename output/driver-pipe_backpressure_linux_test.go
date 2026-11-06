//go:build test_unit && linux

package output

import (
	"os"
	"syscall"
	"testing"
	"time"
)

type signalledSilence struct{ read chan struct{} }

func (s signalledSilence) Read(samples []float32) (int, error) {
	select {
	case s.read <- struct{}{}:
	default:
	}
	clear(samples)
	return len(samples), nil
}

// A disconnected consumer may leave its FIFO open without draining it.
// Control operations must still complete when the kernel pipe buffer is full.
func TestPipeOutputControlsWithStalledReader(t *testing.T) {
	for _, wait := range []bool{false, true} {
		name := "nonblocking-open"
		if wait {
			name = "waiting-open"
		}
		t.Run(name, func(t *testing.T) {
			path := mkfifo(t)
			reader, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			// One output chunk is 8192 bytes, so its first write must block.
			if _, _, errno := syscall.Syscall(syscall.SYS_FCNTL, reader.Fd(), syscall.F_SETPIPE_SZ, 4096); errno != 0 {
				t.Fatal(errno)
			}
			reads := make(chan struct{}, 1)
			opts := pipeOutputOptions(path, wait)
			opts.Reader = signalledSilence{reads}
			out, err := newPipeOutput(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer out.Close()
			select {
			case <-reads:
			case <-time.After(time.Second):
				t.Fatal("output never read a sample chunk")
			}
			// Let the first write reach backpressure before invoking controls.
			time.Sleep(50 * time.Millisecond)
			done := make(chan error, 1)
			go func() {
				if err := out.Pause(); err != nil {
					done <- err
					return
				}
				if err := out.Resume(); err != nil {
					done <- err
					return
				}
				done <- out.Close()
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				// Release an unpatched writer before failing, so cleanup cannot hang.
				reader.Close()
				<-done
				t.Fatal("pause/resume/close blocked behind an undrained audio pipe")
			}
		})
	}
}
