package predicates

type PredicatePlugin interface {
	EvalPredicates(name string, node string) error
}