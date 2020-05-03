# Frequently Asked Questions

### Is `lovefield-ts` sponsored by Google?

No. This is only by Arthur himself as of May 2020. If you were a director or
above in Google wanted to sponsor this project, feel free to have your ABP throw
something on my calendar.

### Is `lovefield-ts` production quality?

Arthur believes he had found all existing bugs regarding to this TypeScript
port. Current unit test coverage is just a little bit over 90% and they are all
green. Performance benchmark shows similar performance profile as [the original
Lovefield](https://github.com/google/lovefield).

Just like other Apache-2.0 licensed open source projects, use as-is and
assume risks with it.

### What are the dependencies of `lovefield-ts`?

None. It's deliberately built so.

### How's the performance of `lovefield-ts`?

It's GOOD. Any modern PC/browser can easily handle 500MB of data using
`lovefield-ts` like a breeze.

### Can I use it with Chrome/Edge/Safari?

Yes, as long as they are new enough with full ES6 support. As to other browsers,
please just try yourself. Arthur does not have time to install them at all.

### Why do you build this?

Arthur built this originally in 2018 to learn modern TypeScript. It has been
revived in 2020 because of the shelter-in-place order in California gives back
him 2-hours of commute time per-day.

A side reason is that Arthur is fed up with the slow Closure compiler, which
Google mandated to use when he started this project.
