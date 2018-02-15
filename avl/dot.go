package avl

import (
	"fmt"
	"io"
	"text/template"
)

type graphEdge struct {
	From, To string
}

type graphNode struct {
	Path  string
	Label string
	Value string
	Attrs map[string]string
}

type graphContext struct {
	Edges []*graphEdge
	Nodes []*graphNode
}

type Graphable interface {
	Dot() *graphContext
}

var graphTemplate = `
strict graph {
	{{- range $i, $edge := $.Edges}}
	"{{ $edge.From }}" -- "{{ $edge.To }}";
	{{- end}}

	{{range $i, $node := $.Nodes}}
	"{{ $node.Path }}" [label=<{{ $node.Label }}>,{{ range $k, $v := $node.Attrs }}{{ $k }}={{ $v }},{{end}}];
	{{- end}}
}
`

var defaultGraphNodeAttrs = map[string]string{
	"shape": "circle",
}

func mkLabel(label string, pt int, face string) string {
	return fmt.Sprintf("<font face='%s' point-size='%d'>%s</font><br />", face, pt, label)
}

func DotGraph(title string, g Graphable, w io.Writer) error {
	ctx := g.Dot()
	var tpl = template.Must(template.New(title).Parse(graphTemplate))
	return tpl.Execute(w, ctx)
}
