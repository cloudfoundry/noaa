# eachers
[Gomega](http://github.com/onsi/gomega) matchers that integrate with [hel](http://github.com/nelsam/hel) mocks.

hel uses channels to store the history of a method. This allows tests to assert that the file was called any number of times in a go-routine safe way. 

While Gomega has matchers for channels (e.g. [Receive](http://onsi.github.io/gomega/#receive)), they don't lend themselves to asserting for multiple values. *eachers* is intended to fill that void.

## Provided Matchers ##

###`EqualEach`###
```
Expect(helChannel).To(EqualEach(1,2,3))
```

###`BeEquivalentToEach`###
```
Expect(helChannel).To(BeEquivalentToEach(1,2,3))
```

###`Each`###
```
Expect(helChannel).To(Each(BeEquivalentTo,1,2,3))
```

Note: `Each` is actually used by both `EqualEach` and `BeEquivalentToEach`.
