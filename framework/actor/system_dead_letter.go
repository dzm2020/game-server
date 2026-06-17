package actor

//func (s *System) DeadLetters() <-chan DeadLetter {
//	return s.deadLetters
//}
//
//func (s *System) DeadLetterDropped() uint64 {
//	return s.metrics.snapshot().DeadLetterDropped
//}
//
//func (s *System) publishDeadLetter(from PID, target any, msg any, err error) {
//	if !errors.Is(err, ErrMailboxFull) && !errors.Is(err, ErrStopped) && !errors.Is(err, ErrActorNotFound) {
//		return
//	}
//	if errors.Is(err, ErrMailboxFull) {
//		s.metrics.incMailboxFull()
//	}
//	s.metrics.incDeadLetter()
//	letter := DeadLetter{
//		Time:    time.Now(),
//		From:    from,
//		Target:  target,
//		Message: msg,
//		Error:   err,
//	}
//	select {
//	case s.deadLetters <- letter:
//	default:
//		s.metrics.incDeadLetterDropped()
//	}
//}
