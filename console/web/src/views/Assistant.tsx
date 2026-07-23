import { useEffect, useRef, useState } from "react";
import * as api from "../lib/api";
import { Banner, Button, Card, Spinner, Tag } from "../components/ui";
import { Markdown } from "../lib/markdown";

type ChatMessage = api.AssistantMessage & { trace?: api.AssistantToolTrace[] };

// Persisted per connection so leaving the tab or reloading keeps the conversation.
function conversationKey(name: string): string {
  return `meshdb.assistant.${name}`;
}

function readMessages(key: string): ChatMessage[] {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return [];
    const value = JSON.parse(raw);
    if (!Array.isArray(value)) return [];
    return value.flatMap((item) =>
      item && typeof item === "object" && (item.role === "user" || item.role === "assistant") && typeof item.content === "string"
        ? [{ role: item.role, content: item.content, trace: Array.isArray(item.trace) ? item.trace : undefined }]
        : [],
    );
  } catch {
    return [];
  }
}

export default function Assistant({ name }: { name: string }) {
  const storageKey = conversationKey(name);
  const [messages, setMessages] = useState<ChatMessage[]>(() => readMessages(storageKey));
  const [pending, setPending] = useState<api.AssistantPending | null>(null);
  const [resume, setResume] = useState<unknown>(undefined);
  const [confirming, setConfirming] = useState(false);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notConfigured, setNotConfigured] = useState(false);
  const scroller = useRef<HTMLDivElement>(null);

  useEffect(() => {
    scroller.current?.scrollTo({ top: scroller.current.scrollHeight });
  }, [messages, busy, pending]);

  useEffect(() => {
    try { localStorage.setItem(storageKey, JSON.stringify(messages)); } catch { /* browser storage can be unavailable */ }
  }, [storageKey, messages]);

  // Fold a reply into UI state: an `answer` becomes a stored assistant message; a `pending`
  // change is held as transient confirm-card state until the user decides.
  const applyReply = (reply: api.AssistantReply, base: ChatMessage[]) => {
    if (typeof reply.answer === "string") {
      setMessages([...base, { role: "assistant", content: reply.answer, trace: reply.trace }]);
      setPending(null);
      setResume(undefined);
    } else if (reply.pending) {
      setMessages(base);
      setPending(reply.pending);
      setResume(reply.resume);
    }
  };

  const reportError = (caught: unknown) => {
    if (caught instanceof api.ApiError && caught.status === 409) {
      setNotConfigured(true);
    } else {
      setError(caught instanceof Error ? caught.message : "The assistant request failed.");
    }
  };

  const send = async (e: React.FormEvent) => {
    e.preventDefault();
    const content = input.trim();
    if (!content || busy || confirming) return;
    const outgoing: ChatMessage[] = [...messages, { role: "user", content }];
    setMessages(outgoing);
    setPending(null);
    setResume(undefined);
    setInput("");
    setBusy(true);
    setError(null);
    setNotConfigured(false);
    try {
      const reply = await api.conn(name).assistant(outgoing.map((m) => ({ role: m.role, content: m.content })));
      applyReply(reply, outgoing);
    } catch (caught) {
      reportError(caught);
    } finally {
      setBusy(false);
    }
  };

  const confirmPending = async () => {
    if (!pending || confirming) return;
    setConfirming(true);
    setError(null);
    try {
      const reply = await api.conn(name).assistantConfirm(pending, resume);
      applyReply(reply, messages);
    } catch (caught) {
      reportError(caught);
    } finally {
      setConfirming(false);
    }
  };

  const rejectPending = () => {
    setPending(null);
    setResume(undefined);
    setMessages([...messages, { role: "assistant", content: "(change cancelled)" }]);
  };

  const clearConversation = () => {
    setMessages([]);
    setPending(null);
    setResume(undefined);
    setError(null);
    setNotConfigured(false);
  };

  return (
    <div className="flex h-full min-h-0 flex-col bg-carbon-bg">
      <div className="flex h-12 shrink-0 items-center justify-between border-b border-carbon-border bg-carbon-layer px-4">
        <span className="text-sm font-semibold text-carbon-text">Assistant</span>
        <div className="flex items-center gap-3">
          <span className="font-mono text-xs text-carbon-text-3">{name}</span>
          <Button
            variant="secondary"
            className="min-h-0 px-3 py-1 text-xs"
            disabled={messages.length === 0 && !pending}
            onClick={clearConversation}
          >
            Clear conversation
          </Button>
        </div>
      </div>

      <div ref={scroller} className="min-h-0 flex-1 overflow-y-auto p-4">
        <div className="mx-auto max-w-3xl space-y-4">
          {messages.length === 0 && !pending && !notConfigured && !error && (
            <p className="py-10 text-center text-sm text-carbon-text-3">
              Ask about this database, or ask for a change (e.g. "create a table…"). The assistant reads evidence on your behalf and never changes data or schema without your confirmation.
            </p>
          )}
          {messages.map((message, index) => <MessageBubble key={index} message={message} />)}
          {busy && <div className="flex justify-start"><div className="max-w-[85%] border border-carbon-border bg-carbon-layer px-3 py-2"><Spinner label="Thinking…" /></div></div>}
          {pending && (
            <div className="flex justify-start">
              <Card
                className="w-full max-w-[85%] border-l-2 border-l-carbon-yellow"
                title={<Tag tone="yellow">Confirmation required</Tag>}
              >
                <p className="text-sm text-carbon-text">The assistant wants to make a change to this database:</p>
                <pre className="mt-2 whitespace-pre-wrap border border-carbon-border bg-carbon-field px-2 py-1.5 font-mono text-xs text-carbon-text-2">{pending.summary}</pre>
                <div className="mt-3 flex gap-2">
                  <Button variant="primary" disabled={confirming} onClick={confirmPending}>{confirming ? "Applying…" : "Confirm"}</Button>
                  <Button variant="secondary" disabled={confirming} onClick={rejectPending}>Reject</Button>
                </div>
              </Card>
            </div>
          )}
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
            disabled={busy || confirming}
            onChange={(e) => setInput(e.target.value)}
          />
          <Button type="submit" disabled={busy || confirming || !input.trim()}>{busy ? "Sending…" : "Send"}</Button>
        </div>
      </form>
    </div>
  );
}

function MessageBubble({ message }: { message: ChatMessage }) {
  const user = message.role === "user";
  return (
    <div className={`flex ${user ? "justify-end" : "justify-start"}`}>
      <div className={`max-w-[85%] px-3 py-2 text-sm ${user ? "whitespace-pre-wrap bg-carbon-blue text-white" : "border border-carbon-border bg-carbon-layer text-carbon-text"}`}>
        {user ? message.content : <Markdown text={message.content} />}
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
