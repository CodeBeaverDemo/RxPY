import unittest
from typing import List

import reactivex
from reactivex import operators as ops
from reactivex.observable.observable import Observable
from reactivex.testing import ReactiveTest, TestScheduler
from reactivex.testing.recorded import Recorded

on_next = ReactiveTest.on_next
on_completed = ReactiveTest.on_completed
on_error = ReactiveTest.on_error
subscribe = ReactiveTest.subscribe
subscribed = ReactiveTest.subscribed
disposed = ReactiveTest.disposed
created = ReactiveTest.created


class TestCombineThrottle(unittest.TestCase):
    def test_combine_throttle_never_never(self):
        scheduler = TestScheduler()
        o1 = reactivex.never()
        o2 = reactivex.never()

        def create():
            return o1.pipe(ops.combine_throttle(o2))

        results = scheduler.start(create)
        assert results.messages == []

    def test_combine_throttle_never_empty(self):
        scheduler = TestScheduler()
        msgs = [on_next(150, 1), on_completed(210)]
        o1 = reactivex.never()
        o2 = scheduler.create_hot_observable(msgs)

        def create():
            return o1.pipe(ops.combine_throttle(o2))

        results = scheduler.start(create)
        assert results.messages == [on_completed(210)]

    def test_combine_throttle_empty_empty(self):
        scheduler = TestScheduler()
        msgs1 = [on_next(150, 1), on_completed(210)]
        msgs2 = [on_next(150, 1), on_completed(210)]
        e1 = scheduler.create_hot_observable(msgs1)
        e2 = scheduler.create_hot_observable(msgs2)

        def create():
            return e1.pipe(ops.combine_throttle(e2))

        results = scheduler.start(create)
        assert results.messages == [on_completed(210)]

    def test_combine_throttle_empty_non_empty(self):
        scheduler = TestScheduler()
        msgs1 = [on_next(150, 1), on_completed(210)]
        msgs2 = [on_next(150, 1), on_next(215, 2), on_completed(220)]
        e1 = scheduler.create_hot_observable(msgs1)
        e2 = scheduler.create_hot_observable(msgs2)

        def create():
            return e1.pipe(ops.combine_throttle(e2))

        results = scheduler.start(create)
        assert results.messages == [on_completed(210)]

    def test_combine_throttle_non_empty_empty(self):
        scheduler = TestScheduler()
        msgs1 = [on_next(150, 1), on_completed(210)]
        msgs2 = [on_next(150, 1), on_next(215, 2), on_completed(220)]
        e1 = scheduler.create_hot_observable(msgs1)
        e2 = scheduler.create_hot_observable(msgs2)

        def create():
            return e2.pipe(ops.combine_throttle(e1))

        results = scheduler.start(create)
        assert results.messages == [on_completed(210)]

    def test_combine_throttle_never_non_empty(self):
        scheduler = TestScheduler()
        msgs = [on_next(150, 1), on_next(215, 2), on_completed(220)]
        e1 = scheduler.create_hot_observable(msgs)
        e2 = reactivex.never()

        def create():
            return e2.pipe(ops.combine_throttle(e1))

        results = scheduler.start(create)
        assert results.messages == [on_completed(220)]

    def test_combine_throttle_non_empty_never(self):
        scheduler = TestScheduler()
        msgs = [on_next(150, 1), on_next(215, 2), on_completed(220)]
        e1 = scheduler.create_hot_observable(msgs)
        e2 = reactivex.never()

        def create():
            return e1.pipe(ops.combine_throttle(e2))

        results = scheduler.start(create)
        assert results.messages == [on_completed(220)]

    def test_combine_throttle_non_empty_non_empty(self):
        scheduler = TestScheduler()
        msgs1 = [on_next(150, 1), on_next(215, 2), on_completed(230)]
        msgs2 = [on_next(150, 1), on_next(220, 3), on_completed(240)]
        e1 = scheduler.create_hot_observable(msgs1)
        e2 = scheduler.create_hot_observable(msgs2)

        def create():
            return e1.pipe(ops.combine_throttle(e2))

        results = scheduler.start(create)
        assert results.messages == [on_next(220, (2, 3)), on_completed(230)]

    def test_combine_throttle_empty_error(self):
        ex = Exception("ex")
        scheduler = TestScheduler()
        msgs1 = [on_next(150, 1), on_completed(230)]
        msgs2 = [on_next(150, 1), on_error(220, ex)]
        e1 = scheduler.create_hot_observable(msgs1)
        e2 = scheduler.create_hot_observable(msgs2)

        def create():
            return e1.pipe(ops.combine_throttle(e2))

        results = scheduler.start(create)
        assert results.messages == [on_error(220, ex)]

    def test_combine_throttle_error_empty(self):
        ex = Exception("ex")
        scheduler = TestScheduler()
        msgs1 = [on_next(150, 1), on_completed(230)]
        msgs2 = [on_next(150, 1), on_error(220, ex)]
        e1 = scheduler.create_hot_observable(msgs1)
        e2 = scheduler.create_hot_observable(msgs2)

        def create():
            return e2.pipe(ops.combine_throttle(e1))

        results = scheduler.start(create)
        assert results.messages == [on_error(220, ex)]

    def test_combine_throttle_never_error(self):
        ex = Exception("ex")
        scheduler = TestScheduler()
        msgs2 = [on_next(150, 1), on_error(220, ex)]
        e1 = reactivex.never()
        e2 = scheduler.create_hot_observable(msgs2)

        def create():
            return e1.pipe(ops.combine_throttle(e2))

        results = scheduler.start(create)
        assert results.messages == [on_error(220, ex)]

    def test_combine_throttle_error_never(self):
        ex = Exception("ex")
        scheduler = TestScheduler()
        msgs2 = [on_next(150, 1), on_error(220, ex)]
        e1 = reactivex.never()
        e2 = scheduler.create_hot_observable(msgs2)

        def create():
            return e2.pipe(ops.combine_throttle(e1))

        results = scheduler.start(create)
        assert results.messages == [on_error(220, ex)]

    def test_combine_throttle_error_error(self):
        ex1 = Exception("ex1")
        ex2 = Exception("ex2")
        scheduler = TestScheduler()
        msgs1 = [on_next(150, 1), on_error(230, ex1)]
        msgs2 = [on_next(150, 1), on_error(220, ex2)]
        e1 = scheduler.create_hot_observable(msgs1)
        e2 = scheduler.create_hot_observable(msgs2)

        def create():
            return e2.pipe(ops.combine_throttle(e1))

        results = scheduler.start(create)
        assert results.messages == [on_error(220, ex2)]

    def test_combine_throttle_some_error(self):
        ex = Exception("ex")
        scheduler = TestScheduler()
        msgs1 = [on_next(150, 1), on_next(215, 2), on_completed(230)]
        msgs2 = [on_next(150, 1), on_error(220, ex)]
        e1 = scheduler.create_hot_observable(msgs1)
        e2 = scheduler.create_hot_observable(msgs2)

        def create():
            return e1.pipe(ops.combine_throttle(e2))

        results = scheduler.start(create)
        assert results.messages == [on_error(220, ex)]

    def test_combine_throttle_error_some(self):
        ex = Exception("ex")
        scheduler = TestScheduler()
        msgs1 = [on_next(150, 1), on_next(215, 2), on_completed(230)]
        msgs2 = [on_next(150, 1), on_error(220, ex)]
        e1 = scheduler.create_hot_observable(msgs1)
        e2 = scheduler.create_hot_observable(msgs2)

        def create():
            return e2.pipe(ops.combine_throttle(e1))

        results = scheduler.start(create)
        assert results.messages == [on_error(220, ex)]

    def test_combine_throttle_different_speeds(self):
        scheduler = TestScheduler()
        msgs1 = [
            on_next(150, 1),
            on_next(215, 2),
            on_next(230, 3),
            on_next(240, 4),
            on_next(290, 5),
            on_completed(310),
        ]
        msgs2 = [
            on_next(150, "a"),
            on_next(210, "b"),
            on_next(250, "c"),
            on_next(270, "d"),
            on_next(280, "e"),
            on_completed(300),
        ]
        e1 = scheduler.create_hot_observable(msgs1)
        e2 = scheduler.create_hot_observable(msgs2)

        def create():
            return e1.pipe(ops.combine_throttle(e2))

        results = scheduler.start(create)
        assert e1.subscriptions == [subscribe(200, 300)]
        assert results.messages == [
            on_next(215, (2, "b")),
            on_next(250, (4, "c")),
            on_next(290, (5, "e")),
            on_completed(300),
        ]

    def test_combine_throttle_one_after_other(self):
        scheduler = TestScheduler()
        msgs1 = [on_next(150, 1), on_next(215, 2), on_next(230, 3), on_completed(240)]
        msgs2 = [on_next(250, "a"), on_next(260, "b"), on_completed(270)]
        e1 = scheduler.create_hot_observable(msgs1)
        e2 = scheduler.create_hot_observable(msgs2)

        def create():
            return e1.pipe(ops.combine_throttle(e2))

        results = scheduler.start(create)
        assert e1.subscriptions == [subscribe(200, 240)]
        assert results.messages == [on_completed(240)]

    def test_combine_throttle_100_observables_with_linearly_increased_speeds(self):
        scheduler = TestScheduler()

        obeservables: List[Observable[int]] = []
        all_msgs: List[List[Recorded[int]]] = []

        for i in range(1, 101):
            msgs: List[Recorded[int]] = []
            for j in range(0, 200, i):
                msgs.append(on_next(201 + j, i))

            msgs.append(on_completed(500))

            obeservables.append(scheduler.create_hot_observable(msgs))
            all_msgs.append(msgs)

        def create():
            return obeservables[0].pipe(ops.combine_throttle(*obeservables[1:]))

        results = scheduler.start(create)

        assert results.messages == [
            on_next(201, tuple(range(1, 101))),
            on_next(301, tuple(range(1, 101))),
            on_completed(500),
        ]

    def test_combine_throttle_single(self):
        """Test combine_throttle with only the source observable and no additional observables.
        Each element should be wrapped in a single-item tuple.
        """
        scheduler = TestScheduler()
        msgs = [on_next(210, 10), on_next(220, 20), on_completed(230)]
        source = scheduler.create_hot_observable(msgs)
        def create():
            return source.pipe(ops.combine_throttle())
        results = scheduler.start(create)
        assert results.messages == [on_next(210, (10,)), on_next(220, (20,)), on_completed(230)]

    def test_combine_throttle_simultaneous(self):
        """Test combine_throttle when both observables emit simultaneously.
        The combined event should occur at the emission time, and completion should be 
        signaled at the earliest completion (here, from the additional observable).
        """
        scheduler = TestScheduler()
        msgs1 = [on_next(210, 'A'), on_completed(250)]
        msgs2 = [on_next(210, 'B'), on_completed(240)]
        e1 = scheduler.create_hot_observable(msgs1)
        e2 = scheduler.create_hot_observable(msgs2)
        def create():
            return e1.pipe(ops.combine_throttle(e2))
        results = scheduler.start(create)
        assert results.messages == [on_next(210, ('A', 'B')), on_completed(240)]

    def test_combine_throttle_three_sources(self):
        """Test combine_throttle with three observable sources.
        Verifies that the first combined event occurs when all sources have emitted at least once,
        and that completion occurs at the earliest termination among the sources.
        """
        scheduler = TestScheduler()
        msgs1 = [on_next(210, 1), on_next(230, 2), on_completed(250)]
        msgs2 = [on_next(220, "x"), on_completed(240)]
        msgs3 = [on_next(215, True), on_next(235, False), on_completed(260)]
        e1 = scheduler.create_hot_observable(msgs1)
        e2 = scheduler.create_hot_observable(msgs2)
        e3 = scheduler.create_hot_observable(msgs3)
        def create():
            return e1.pipe(ops.combine_throttle(e2, e3))
        results = scheduler.start(create)
        # The first tuple is produced when all three have emitted:
        # max(210, 220, 215) = 220, so expect (1, "x", True) and then completion at 240 (from e2)
        assert results.messages == [on_next(220, (1, "x", True)), on_completed(240)]

    def test_combine_throttle_dispose_before_combination(self):
        """Test that combine_throttle does not emit further events after disposal.
        In this case, the disposal time prevents later combinations from being emitted.
        """
        scheduler = TestScheduler()
        msgs1 = [on_next(210, 1), on_next(300, 2), on_completed(400)]
        msgs2 = [on_next(220, "a"), on_next(350, "b"), on_completed(500)]
        e1 = scheduler.create_hot_observable(msgs1)
        e2 = scheduler.create_hot_observable(msgs2)
        def create():
            return e1.pipe(ops.combine_throttle(e2))
        results = scheduler.start(create, disposed=250)
        # Only the first combined event (at max(210,220) = 220) should be output.
        assert results.messages == [on_next(220, (1, "a"))]