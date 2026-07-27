import { forwardRef, useEffect, useImperativeHandle, useRef } from "react";
import { defaultKeymap, history, historyKeymap, indentWithTab } from "@codemirror/commands";
import { bracketMatching, HighlightStyle, indentOnInput, syntaxHighlighting } from "@codemirror/language";
import { sql, SQLite } from "@codemirror/lang-sql";
import { EditorState } from "@codemirror/state";
import { EditorView, highlightActiveLine, highlightActiveLineGutter, keymap, lineNumbers } from "@codemirror/view";
import { tags } from "@lezer/highlight";

export type EditorRange = { from: number; to: number };
export type CodeEditorHandle = { insert: (text: string) => void };

const sqlHighlighting = HighlightStyle.define([
  { tag: tags.keyword, color: "#78a9ff", fontWeight: "600" },
  { tag: [tags.string, tags.special(tags.string)], color: "#42be65" },
  { tag: [tags.number, tags.bool, tags.null], color: "#be95ff" },
  { tag: [tags.lineComment, tags.blockComment], color: "#8d8d8d", fontStyle: "italic" },
  { tag: [tags.operator, tags.operatorKeyword], color: "#ff7eb6" },
  { tag: [tags.typeName, tags.className], color: "#82cfff" },
  { tag: [tags.variableName, tags.propertyName], color: "#f4f4f4" },
  { tag: tags.punctuation, color: "#c6c6c6" },
]);

export const CodeEditor = forwardRef<CodeEditorHandle, {
  value: string;
  onChange: (value: string) => void;
  /// `target` says what the keystroke asked for: Ctrl/Cmd+Enter runs the selection when there is
  /// one and the statement under the cursor otherwise; Ctrl/Cmd+Shift+Enter runs the whole editor.
  onRun: (selection: EditorRange, target: "current" | "selection" | "all") => void;
  onSelectionChange?: (selection: EditorRange) => void;
}>(function CodeEditor({
  value,
  onChange,
  onRun,
  onSelectionChange,
}, forwardedRef) {
  const host = useRef<HTMLDivElement>(null);
  const view = useRef<EditorView | null>(null);
  const change = useRef(onChange);
  const run = useRef(onRun);
  const selectionChange = useRef(onSelectionChange);
  change.current = onChange;
  run.current = onRun;
  selectionChange.current = onSelectionChange;

  useImperativeHandle(forwardedRef, () => ({
    insert(text: string) {
      const editor = view.current;
      if (!editor) return;
      const selection = editor.state.selection.main;
      const cursor = selection.from + text.length;
      editor.dispatch({
        changes: { from: selection.from, to: selection.to, insert: text },
        selection: { anchor: cursor },
        scrollIntoView: true,
      });
      editor.focus();
    },
  }), []);

  useEffect(() => {
    if (!host.current) return;
    const editor = new EditorView({
      parent: host.current,
      state: EditorState.create({
        doc: value,
        extensions: [
          sql({ dialect: SQLite }),
          syntaxHighlighting(sqlHighlighting),
          history(),
          lineNumbers(),
          highlightActiveLine(),
          highlightActiveLineGutter(),
          bracketMatching(),
          indentOnInput(),
          keymap.of([
            {
              key: "Mod-Enter",
              run: (editor) => {
                const main = editor.state.selection.main;
                run.current({ from: main.from, to: main.to }, main.from === main.to ? "current" : "selection");
                return true;
              },
            },
            {
              key: "Mod-Shift-Enter",
              run: (editor) => {
                const main = editor.state.selection.main;
                run.current({ from: main.from, to: main.to }, "all");
                return true;
              },
            },
            indentWithTab,
            ...defaultKeymap,
            ...historyKeymap,
          ]),
          EditorView.lineWrapping,
          EditorView.updateListener.of((update) => {
            if (update.docChanged) change.current(update.state.doc.toString());
            if (update.selectionSet) {
              const main = update.state.selection.main;
              selectionChange.current?.({ from: main.from, to: main.to });
            }
          }),
          EditorView.theme({
            "&": { height: "100%", minHeight: "0", background: "#262626", color: "#f4f4f4" },
            ".cm-scroller": { height: "100%", minHeight: "0", overflow: "auto" },
            ".cm-content": { minHeight: "100%", fontFamily: "IBM Plex Mono, monospace", fontSize: "13px", caretColor: "#78a9ff" },
            ".cm-cursor, .cm-dropCursor": { borderLeftColor: "#78a9ff" },
            ".cm-gutters": { background: "#262626", color: "#6f6f6f", borderRight: "1px solid #393939" },
            ".cm-activeLine, .cm-activeLineGutter": { background: "#333333" },
            ".cm-selectionBackground, &.cm-focused .cm-selectionBackground": { background: "#0f62fe55" },
            "&.cm-focused": { outline: "1px solid #0f62fe" },
          }),
          EditorView.contentAttributes.of({ "aria-label": "SQL editor" }),
        ],
      }),
    });
    view.current = editor;
    const main = editor.state.selection.main;
    selectionChange.current?.({ from: main.from, to: main.to });
    return () => {
      editor.destroy();
      view.current = null;
    };
    // The editor is intentionally created once; subsequent value updates are synchronized below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    const editor = view.current;
    if (!editor || editor.state.doc.toString() === value) return;
    editor.dispatch({ changes: { from: 0, to: editor.state.doc.length, insert: value } });
  }, [value]);

  return <div ref={host} className="h-full min-h-0 overflow-hidden border border-carbon-border" />;
});
