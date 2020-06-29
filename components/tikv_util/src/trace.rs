use kvproto::span as spanpb;
use minitrace::{Link, Span, SpanSet};

pub fn encode_spans(span_sets: Vec<SpanSet>) -> impl Iterator<Item = spanpb::SpanSet> {
    span_sets
        .into_iter()
        .map(|span_set| {
            let mut pb_set = spanpb::SpanSet::default();
            pb_set.set_create_time_ns(span_set.create_time_ns);
            pb_set.set_start_time_ns(span_set.start_time_ns);
            pb_set.set_cycles_per_sec(span_set.cycles_per_sec);

            let spans = span_set.spans.into_iter().map(|span| {
                let mut s = spanpb::Span::default();
                s.set_id(span.id);
                s.set_begin_cycles(span.begin_cycles);
                s.set_end_cycles(span.end_cycles);
                s.set_event(span.event);

                #[cfg(feature = "prost-codec")]
                {
                    s.link = Some(spanpb::Link {
                        link: Some(match span.link {
                            Link::Root => spanpb::link::Link::Root(spanpb::Root {}),
                            Link::Parent { id } => {
                                spanpb::link::Link::Parent(spanpb::Parent { id })
                            }
                            Link::Continue { id } => {
                                spanpb::link::Link::Continue(spanpb::Continue { id })
                            }
                        }),
                    });
                }

                #[cfg(feature = "protobuf-codec")]
                {
                    let mut link = spanpb::Link::new();
                    match span.link {
                        Link::Root => link.set_root(spanpb::Root::new()),
                        Link::Parent { id } => {
                            let mut parent = spanpb::Parent::new();
                            parent.set_id(id);
                            link.set_parent(parent);
                        }
                        Link::Continue { id } => {
                            let mut cont = spanpb::Continue::new();
                            cont.set_id(id);
                            link.set_continue(cont);
                        }
                    };
                    s.set_link(link);
                }
                s
            });

            pb_set.set_spans(spans.collect());

            pb_set
        })
        .into_iter()
}

pub fn decode_spans(span_sets: Vec<spanpb::SpanSet>) -> impl Iterator<Item = SpanSet> {
    span_sets.into_iter().map(|span_set| {
        let spans = span_set
            .spans
            .into_iter()
            .map(|span| {
                #[cfg(feature = "prost-codec")]
                {
                    if let Some(link) = span.link {
                        let link = match link.link {
                            Some(spanpb::link::Link::Root(spanpb::Root {})) => Link::Root,
                            Some(spanpb::link::Link::Parent(spanpb::Parent { id })) => {
                                Link::Parent { id }
                            }
                            Some(spanpb::link::Link::Continue(spanpb::Continue { id })) => {
                                Link::Continue { id }
                            }
                            _ => panic!("Link should not be none from spanpb"),
                        };
                        Span {
                            id: span.id,
                            begin_cycles: span.begin_cycles,
                            end_cycles: span.end_cycles,
                            event: span.event,
                            link,
                        }
                    } else {
                        panic!("Link should not be none from spanpb")
                    }
                }
                #[cfg(feature = "protobuf-codec")]
                {
                    let link = if span.get_link().has_root() {
                        Link::Root
                    } else if span.get_link().has_parent() {
                        Link::Parent {
                            id: span.get_link().get_parent().id,
                        }
                    } else if span.get_link().has_continue() {
                        Link::Continue {
                            id: span.get_link().get_continue().id,
                        }
                    } else {
                        panic!("Link must be one of root, parent or continue")
                    };
                    Span {
                        id: span.id,
                        begin_cycles: span.begin_cycles,
                        end_cycles: span.end_cycles,
                        event: span.event,
                        link,
                    }
                }
            })
            .collect();
        SpanSet {
            create_time_ns: span_set.create_time_ns,
            start_time_ns: span_set.start_time_ns,
            cycles_per_sec: span_set.cycles_per_sec,
            spans,
        }
    })
}

#[cfg(test)]
mod tests {
    use minitrace::{Link, Span, SpanSet};

    #[test]
    fn test_encode_spans() {
        let spans = vec![
            Span {
                id: 0,
                link: Link::Root,
                begin_cycles: 0,
                end_cycles: 10,
                event: 0,
            },
            Span {
                id: 1,
                link: Link::Parent { id: 0 },
                begin_cycles: 0,
                end_cycles: 9,
                event: 1,
            },
        ];
        let raw_span_set = vec![SpanSet {
            create_time_ns: 0,
            start_time_ns: 1,
            cycles_per_sec: 2,
            spans,
        }];

        let spanpb_set_vec = crate::trace::encode_spans(raw_span_set.clone()).collect::<Vec<_>>();
        let encode_and_decode: Vec<_> = crate::trace::decode_spans(spanpb_set_vec).collect();
        assert_eq!(raw_span_set, encode_and_decode)
    }
}
