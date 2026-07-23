// A tiny, dependency-free GitHub-flavored Markdown renderer that builds REACT ELEMENTS.
// It never uses dangerouslySetInnerHTML: the assistant's output is untrusted, so every piece of
// text lands in a React text node (auto-escaped) and any raw HTML shows up as literal characters.
// Scope is deliberately small — headings, emphasis, code, lists, GFM pipe tables, and safe links —
// correctness on those cases matters more than covering every Markdown corner.

import { ReactNode } from "react";
import { DataTable } from "../components/ui";

type InlineToken = { index: number; length: number; node: ReactNode };

// Inline formatting, resolved by scanning for the earliest special run. Order in this list is the
// tie-breaker when two patterns match at the same position, so `**` (bold) is tried before `*`.
function parseInline(text: string, keyPrefix: string): ReactNode[] {
  const nodes: ReactNode[] = [];
  let cursor = 0;
  let key = 0;
  while (cursor < text.length) {
    const rest = text.slice(cursor);
    const token = firstInlineToken(rest, `${keyPrefix}-${key}`);
    if (!token) {
      nodes.push(text.slice(cursor));
      break;
    }
    if (token.index > 0) nodes.push(text.slice(cursor, cursor + token.index));
    nodes.push(token.node);
    cursor += token.index + token.length;
    key++;
  }
  return nodes;
}

function firstInlineToken(text: string, key: string): InlineToken | null {
  const patterns: Array<{ re: RegExp; make: (m: RegExpExecArray) => ReactNode }> = [
    { re: /`([^`]+)`/, make: (m) => <code key={key} className="rounded-sm bg-carbon-field px-1 py-0.5 font-mono text-[0.85em] text-carbon-text-2">{m[1]}</code> },
    { re: /\[([^\]]+)\]\(([^)\s]+)\)/, make: (m) => renderLink(m[1], m[2], key) },
    { re: /\*\*([^*]+)\*\*/, make: (m) => <strong key={key} className="font-semibold text-carbon-text">{parseInline(m[1], key)}</strong> },
    { re: /__([^_]+)__/, make: (m) => <strong key={key} className="font-semibold text-carbon-text">{parseInline(m[1], key)}</strong> },
    { re: /\*([^*]+)\*/, make: (m) => <em key={key}>{parseInline(m[1], key)}</em> },
    { re: /_([^_]+)_/, make: (m) => <em key={key}>{parseInline(m[1], key)}</em> },
  ];
  let best: { index: number; length: number; node: ReactNode } | null = null;
  for (const { re, make } of patterns) {
    const match = re.exec(text);
    if (match && (best === null || match.index < best.index)) {
      best = { index: match.index, length: match[0].length, node: make(match) };
    }
  }
  return best;
}

// Links are rendered as anchors ONLY for http(s) URLs; anything else stays literal text so the
// assistant cannot smuggle javascript: or other schemes into a clickable element.
function renderLink(label: string, url: string, key: string): ReactNode {
  if (!/^https?:\/\//i.test(url)) return `[${label}](${url})`;
  return <a key={key} href={url} target="_blank" rel="noreferrer" className="text-carbon-blue underline">{parseInline(label, key)}</a>;
}

function splitRow(line: string): string[] {
  let cells = line.trim();
  if (cells.startsWith("|")) cells = cells.slice(1);
  if (cells.endsWith("|")) cells = cells.slice(0, -1);
  return cells.split("|").map((cell) => cell.trim());
}

function isTableSeparator(line: string): boolean {
  return /^\s*\|?\s*:?-{1,}:?\s*(\|\s*:?-{1,}:?\s*)*\|?\s*$/.test(line) && line.includes("-");
}

// Block-level parsing: walk the lines once, peeling off whichever block starts here.
export function Markdown({ text }: { text: string }): ReactNode {
  const lines = text.replace(/\r\n?/g, "\n").split("\n");
  const blocks: ReactNode[] = [];
  let i = 0;
  let key = 0;

  while (i < lines.length) {
    const line = lines[i];

    if (line.trim() === "") { i++; continue; }

    // Fenced code block ```lang … ```
    const fence = /^\s*```(\S*)\s*$/.exec(line);
    if (fence) {
      const body: string[] = [];
      i++;
      while (i < lines.length && !/^\s*```\s*$/.test(lines[i])) { body.push(lines[i]); i++; }
      i++; // consume closing fence (if present)
      blocks.push(
        <pre key={key++} className="overflow-x-auto border border-carbon-border bg-carbon-field p-3">
          <code className="font-mono text-xs text-carbon-text-2">{body.join("\n")}</code>
        </pre>,
      );
      continue;
    }

    // Heading #..######
    const heading = /^(#{1,6})\s+(.*)$/.exec(line);
    if (heading) {
      const level = heading[1].length;
      const sizes = ["text-xl", "text-lg", "text-base", "text-sm", "text-sm", "text-xs"];
      const Tag = `h${level}` as "h1" | "h2" | "h3" | "h4" | "h5" | "h6";
      blocks.push(<Tag key={key++} className={`font-semibold text-carbon-text ${sizes[level - 1]}`}>{parseInline(heading[2], `h${key}`)}</Tag>);
      i++;
      continue;
    }

    // GFM pipe table: header row, separator row, then body rows.
    if (/\|/.test(line) && i + 1 < lines.length && isTableSeparator(lines[i + 1])) {
      const header = splitRow(line);
      i += 2;
      const rows: ReactNode[][] = [];
      while (i < lines.length && lines[i].trim() !== "" && lines[i].includes("|")) {
        rows.push(splitRow(lines[i]).map((cell, c) => <span key={c}>{parseInline(cell, `td${key}-${rows.length}-${c}`)}</span>));
        i++;
      }
      blocks.push(<div key={key++}><DataTable columns={header} rows={rows} /></div>);
      continue;
    }

    // Unordered list
    if (/^\s*[-*+]\s+/.test(line)) {
      const items: ReactNode[] = [];
      while (i < lines.length && /^\s*[-*+]\s+/.test(lines[i])) {
        const content = lines[i].replace(/^\s*[-*+]\s+/, "");
        items.push(<li key={items.length}>{parseInline(content, `ul${key}-${items.length}`)}</li>);
        i++;
      }
      blocks.push(<ul key={key++} className="list-disc space-y-1 pl-5">{items}</ul>);
      continue;
    }

    // Ordered list
    if (/^\s*\d+\.\s+/.test(line)) {
      const items: ReactNode[] = [];
      while (i < lines.length && /^\s*\d+\.\s+/.test(lines[i])) {
        const content = lines[i].replace(/^\s*\d+\.\s+/, "");
        items.push(<li key={items.length}>{parseInline(content, `ol${key}-${items.length}`)}</li>);
        i++;
      }
      blocks.push(<ol key={key++} className="list-decimal space-y-1 pl-5">{items}</ol>);
      continue;
    }

    // Paragraph: consecutive non-blank lines that don't start another block. Single newlines
    // inside the paragraph become <br/>.
    const para: string[] = [];
    while (
      i < lines.length &&
      lines[i].trim() !== "" &&
      !/^\s*```/.test(lines[i]) &&
      !/^(#{1,6})\s+/.test(lines[i]) &&
      !/^\s*[-*+]\s+/.test(lines[i]) &&
      !/^\s*\d+\.\s+/.test(lines[i]) &&
      !(/\|/.test(lines[i]) && i + 1 < lines.length && isTableSeparator(lines[i + 1]))
    ) {
      para.push(lines[i]);
      i++;
    }
    blocks.push(
      <p key={key++} className="leading-6">
        {para.flatMap((paraLine, index) => {
          const parsed = parseInline(paraLine, `p${key}-${index}`);
          return index === 0 ? parsed : [<br key={`br${index}`} />, ...parsed];
        })}
      </p>,
    );
  }

  return <div className="space-y-3 text-sm text-carbon-text">{blocks}</div>;
}
