package mailroom

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/nyaruka/mailroom/core/queue"

	"github.com/sirupsen/logrus"
)

// Foreman takes care of managing our set of workers and assigns msgs for each to send
type Foreman struct {
	mr               *Mailroom
	queue            string
	workers          []*Worker
	availableWorkers chan *Worker
	quit             chan bool
}

// NewForeman creates a new Foreman for the passed in server with the number of max workers
func NewForeman(mr *Mailroom, queue string, maxWorkers int) *Foreman {
	foreman := &Foreman{
		mr:               mr,
		queue:            queue,
		workers:          make([]*Worker, maxWorkers),
		availableWorkers: make(chan *Worker, maxWorkers),
		quit:             make(chan bool),
	}

	for i := 0; i < maxWorkers; i++ {
		foreman.workers[i] = NewWorker(foreman, i)
	}

	return foreman
}

// Start starts the foreman and all its workers, assigning jobs while there are some
func (f *Foreman) Start() {
	for _, worker := range f.workers {
		worker.Start()
	}
	go f.Assign()
}

// Stop stops the foreman and all its workers, the wait group of the worker can be used to track progress
func (f *Foreman) Stop() {
	for _, worker := range f.workers {
		worker.Stop()
	}
	close(f.quit)
	logrus.WithField("comp", "foreman").WithField("state", "stopping").Info("foreman stopping")
}

// Assign is our main loop for the Foreman, it takes care of popping the next outgoing task from our
// backend and assigning them to workers
func (f *Foreman) Assign() {
	f.mr.WaitGroup.Add(1)
	defer f.mr.WaitGroup.Done()
	log := logrus.WithField("comp", "foreman")

	log.WithFields(logrus.Fields{
		"state":   "started",
		"workers": len(f.workers),
		"queue":   f.queue,
	}).Info("workers started and waiting")

	lastSleep := false

	for true {
		select {
		// return if we have been told to stop
		case <-f.quit:
			log.WithField("state", "stopped").Info("foreman stopped")
			return

		// otherwise, grab the next task and assign it to a worker
		case worker := <-f.availableWorkers:
			// see if we have a task to work on
			rc := f.mr.RP.Get()
			task, err := queue.PopNextTask(rc, f.queue)
			rc.Close()

			if err == nil && task != nil {
				// if so, assign it to our worker
				worker.job <- task
				lastSleep = false
			} else {
				// we received an error getting the next message, log it
				if err != nil {
					log.WithError(err).Error("error popping task")
				}

				// add our worker back to our queue and sleep a bit
				if !lastSleep {
					log.Debug("sleeping, no tasks")
					lastSleep = true
				}
				f.availableWorkers <- worker
				time.Sleep(250 * time.Millisecond)
			}
		}
	}
}

// Worker is our type for a single goroutine that is handling queued events
type Worker struct {
	id      int
	foreman *Foreman
	job     chan *queue.Task
	log     *logrus.Entry
}

// NewWorker creates a new worker responsible for working on events
func NewWorker(foreman *Foreman, id int) *Worker {
	worker := &Worker{
		id:      id,
		foreman: foreman,
		job:     make(chan *queue.Task, 1),
	}
	return worker
}

// Start starts our Worker's goroutine and has it start waiting for tasks from the foreman
func (w *Worker) Start() {
	go func() {
		w.foreman.mr.WaitGroup.Add(1)
		defer w.foreman.mr.WaitGroup.Done()

		log := logrus.WithField("queue", w.foreman.queue).WithField("worker_id", w.id)
		log.Debug("started")

		for true {
			// list ourselves as available for work
			w.foreman.availableWorkers <- w

			// grab our next piece of work
			task := <-w.job

			// exit if we were stopped
			if task == nil {
				log.Debug("stopped")
				return
			}

			w.handleTask(task)
		}
	}()
}

// Stop stops our worker
func (w *Worker) Stop() {
	close(w.job)
}

func (w *Worker) handleTask(task *queue.Task) {
	log := logrus.WithField("queue", w.foreman.queue).WithField("worker_id", w.id).WithField("task_type", task.Type).WithField("org_id", task.OrgID)

	defer func() {
		// catch any panics and recover
		panicLog := recover()
		if panicLog != nil {
			debug.PrintStack()
			log.WithField("task", string(task.Task)).WithField("task_type", task.Type).WithField("org_id", task.OrgID).Errorf("panic handling task: %s", panicLog)
		}

		// mark our task as complete
		rc := w.foreman.mr.RP.Get()
		err := queue.MarkTaskComplete(rc, w.foreman.queue, task.OrgID)
		if err != nil {
			log.WithError(err)
		}
		rc.Close()
	}()

	log.Info("starting handling of task")
	start := time.Now()

	taskFunc, found := taskFunctions[task.Type]
	if found {
		err := taskFunc(context.Background(), w.foreman.mr, task)
		if err != nil {
			log.WithError(err).WithField("task", string(task.Task)).WithField("task_type", task.Type).WithField("org_id", task.OrgID).Error("error running task")
		}
	} else {
		log.Error("unable to find function for task type")
	}

	log.WithField("elapsed", time.Since(start)).Info("task complete")
}
