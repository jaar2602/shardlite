import { useEffect, useRef, useState } from "react";
import * as api from "../lib/api";
import { Banner, Button, Spinner, Tag } from "../components/ui";

type ChatMessage = api.AssistantMessage & { trace?: api.AssistantToolTrace[] };

export default function Assistant({ name }: { name: string }) {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notConfigured, setNotConfigured] = useState(false);
  const scroller = useRef<HTMLDivElement>(null);

  useEffect(() => {
    scroller.current?.scrollTo({ top: scroller.current.scrollHeight });
  }, [messages, busy]);

  const send = async (e: React.FormEvent) => {
    e.preventDefault();
    const content = input.trim();
    if (!content || busy) return;
    const outgoing: ChatMessage[] = [...messages, { role: "user", content }];
    setMessages(outgoing);
    setInput("");
    setBusy(true);
    setError(null);
    setNotConfigured(false);
    try {
      const reply = await api.conn(name).assistant(outgoing.map((m) => ({ role: m.role, content: m.content })));
      setMessages([...outgoing, { role: "assistant", content: reply.answer, trace: reply.trace }]);
    } catch (caught) {
      if (caught instanceof api.ApiError && caught.status === 409) {
        setNotConfigured(true);
      } else {
        setError(caught instanceof Error ? caught.message : "The assistant request failed.");
      }
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="flex h-full min-h-0 flex-col bg-carbon-bg">
      <div className="flex h-12 shrink-0 items-center justify-between border-b border-carbon-border bg-carbon-layer px-4">
        <span className="text-sm font-semibold text-carbon-text">Assistant</span>
        <span className="font-mono text-xs text-carbon-text-3">{name}</span>
      </div>

      <div ref={scroller} className="min-h-0 flex-1 overflow-y-auto p-4">
        <div className="mx-auto max-w-3xl space-y-4">
          {messages.length === 0 && !notConfigured && !error && (
            <p className="py-10 text-center text-sm text-carbon-text-3">
              Ask a question about this database. The assistant reads evidence on your behalf; it does not change data.
            </p>
          )}
          {messages.map((message, index) => <MessageBubble key={index} message={message} />)}
          {busy && <div className="flex justify-start"><div className="max-w-[85%] border border-carbon-border bg-carbon-layer px-3 py-2"><Spinner label="Thinking…" /></div></div>}
          {notConfigured && (
            <Banner tone="info">
              The assistant is not configured. An admin must set an OpenAI-compatible provider in <a className="text-carbon-blue underline" href="/ai-settings">AI settings</a> and enable it.
            </Banner>
          )}
          {error && <Banner tone="error">{error}</Banner>}
        </div>
      </div>

      <form onSubmit={send} className="shrink-0 border-t border-carbon-border bg-carbon-layer px-4 py-3">
        <div className="mx-auto flex max-w-3xl items-end gap-2">
          <input
            aria-label="Message the assistant"
            className="min-h-9 flex-1 border-b border-carbon-text-3 bg-carbon-field px-3 py-2 text-sm text-carbon-text outline-none placeholder:text-carbon-text-3 focus:border-carbon-blue"
            placeholder="Ask about this database…"
            value={input}
            disabled={busy}
            onChange={(e) => setInput(e.target.value)}
          />
          <Button type="submit" disabled={busy || !input.trim()}>{busy ? "Sending…" : "Send"}</Button>
        </div>
      </form>
    </div>
  );
}

function MessageBubble({ message }: { message: ChatMessage }) {
  const user = message.role === "user";
  return (
    <div className={`flex ${user ? "justify-end" : "justify-start"}`}>
      <div className={`max-w-[85%] whitespace-pre-wrap px-3 py-2 text-sm ${user ? "bg-carbon-blue text-white" : "border border-carbon-border bg-carbon-layer text-carbon-text"}`}>
        {message.content}
        {message.trace && message.trace.length > 0 && (
          <div className="mt-2 flex flex-wrap gap-1.5 border-t border-carbon-border pt-2">
            {message.trace.map((call, index) => <ToolChip key={index} call={call} />)}
          </div>
        )}
      </div>
    </div>
  );
}

function ToolChip({ call }: { call: api.AssistantToolTrace }) {
  const [open, setOpen] = useState(false);
  return (
    <div>
      <button type="button" onClick={() => setOpen((value) => !value)} className="inline-flex items-center gap-1">
        <Tag tone={call.ok ? "green" : "red"}>{call.ok ? "✓" : "✗"} {call.name}</Tag>
      </button>
      {open && <div className="mt-1 border border-carbon-border bg-carbon-field px-2 py-1 font-mono text-xs text-carbon-text-2">{call.summary}</div>}
    </div>
  );
}
