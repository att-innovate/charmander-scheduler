package scheduler

import (
	"github.com/att-innovate/charmander-scheduler/manager"
	"github.com/att-innovate/charmander-scheduler/mesosproto"
)

type Scheduler struct {

	// Invoked when the scheduler successfully registers with a Mesos master.
	Registered func(manager manager.Manager, frameworkId string)

	// Invoked when resources have been offered to this framework.
	ResourceOffers func(manager.Manager, []*mesosproto.Offer)

}
