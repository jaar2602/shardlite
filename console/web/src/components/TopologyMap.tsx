import { KeyboardEvent } from "react";
import { MemberStatus } from "../lib/api";

export interface TopologyNode {
  id: string;
  address?: string | null;
  status: MemberStatus;
  role: string;
  isLeader: boolean;
  isCurrent: boolean;
  dataShare: number;
}

export interface StatusStyle {
  label: string;
  color: string;
  text: string;
  background: string;
}

export const STATUS_STYLES: Record<MemberStatus, StatusStyle> = {
  up: { label: "UP", color: "#42be65", text: "#42be65", background: "rgba(36, 161, 72, .16)" },
  suspected: { label: "SUSPECTED", color: "#f1c21b", text: "#f1c21b", background: "rgba(241, 194, 27, .14)" },
  down: { label: "DOWN", color: "#fa4d56", text: "#fa4d56", background: "rgba(250, 77, 86, .14)" },
  unknown: { label: "UNKNOWN", color: "#8d8d8d", text: "#c6c6c6", background: "rgba(141, 141, 141, .14)" },
};

interface PositionedNode extends TopologyNode {
  x: number;
  y: number;
}

function positions(nodes: TopologyNode[]): PositionedNode[] {
  if (nodes.length === 0) return [];
  if (nodes.length === 1) return [{ ...nodes[0], x: 315, y: 220 }];

  const columns = Math.ceil(Math.sqrt(nodes.length));
  const raw = nodes.map((node, index) => {
    const row = Math.floor(index / columns);
    const column = index % columns;
    return {
      ...node,
      x: (column - row) * 145,
      y: (column + row) * 72,
    };
  });
  const minX = Math.min(...raw.map((node) => node.x));
  const maxX = Math.max(...raw.map((node) => node.x));
  const minY = Math.min(...raw.map((node) => node.y));
  const maxY = Math.max(...raw.map((node) => node.y));
  const offsetX = 305 - (minX + maxX) / 2;
  const offsetY = 205 - (minY + maxY) / 2;

  return raw
    .map((node) => ({ ...node, x: node.x + offsetX, y: node.y + offsetY }))
    .sort((a, b) => a.y - b.y);
}

function points(values: [number, number][]): string {
  return values.map(([x, y]) => `${x},${y}`).join(" ");
}

function Cuboid({
  x,
  y,
  width,
  depth,
  height,
  top,
  left,
  right,
  opacity = 1,
}: {
  x: number;
  y: number;
  width: number;
  depth: number;
  height: number;
  top: string;
  left: string;
  right: string;
  opacity?: number;
}) {
  const half = width / 2;
  const halfDepth = depth / 2;
  const topY = y - height;
  return (
    <g opacity={opacity}>
      <polygon
        points={points([
          [x, topY - halfDepth],
          [x + half, topY],
          [x, topY + halfDepth],
          [x - half, topY],
        ])}
        fill={top}
      />
      <polygon
        points={points([
          [x - half, topY],
          [x, topY + halfDepth],
          [x, y + halfDepth],
          [x - half, y],
        ])}
        fill={left}
      />
      <polygon
        points={points([
          [x + half, topY],
          [x, topY + halfDepth],
          [x, y + halfDepth],
          [x + half, y],
        ])}
        fill={right}
      />
    </g>
  );
}

function NodeStack({
  node,
  selected,
  onSelect,
}: {
  node: PositionedNode;
  selected: boolean;
  onSelect: (id: string) => void;
}) {
  const dataHeight = node.dataShare === 0 ? 10 : Math.min(40, 14 + node.dataShare * 0.26);
  const status = STATUS_STYLES[node.status];
  const activate = (event: KeyboardEvent<SVGGElement>) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      onSelect(node.id);
    }
  };
  const opacity = node.status === "down" ? 0.5 : node.status === "unknown" ? 0.72 : 1;

  return (
    <g
      role="button"
      tabIndex={0}
      aria-label={`Node ${node.id}, ${status.label}, ${node.dataShare} percent data responsibility`}
      className="cursor-pointer outline-none"
      onClick={() => onSelect(node.id)}
      onKeyDown={activate}
    >
      <title>
        {node.id} · {status.label} · {node.dataShare}% data responsibility
      </title>

      {selected && (
        <polygon
          points={points([
            [node.x, node.y - 19],
            [node.x + 67, node.y + 15],
            [node.x, node.y + 49],
            [node.x - 67, node.y + 15],
          ])}
          fill="none"
          stroke="#78a9ff"
          strokeWidth="2"
          opacity="0.95"
        />
      )}

      <Cuboid
        x={node.x}
        y={node.y + 22}
        width={112}
        depth={55}
        height={13}
        top="#525252"
        left="#303030"
        right="#414141"
        opacity={opacity}
      />
      <Cuboid
        x={node.x}
        y={node.y + 5}
        width={92}
        depth={48}
        height={dataHeight}
        top="#4589ff"
        left="#1f57a4"
        right="#2f6fce"
        opacity={opacity}
      />
      <Cuboid
        x={node.x}
        y={node.y - dataHeight - 1}
        width={78}
        depth={40}
        height={13}
        top={node.isLeader ? "#a56eff" : "#8a3ffc"}
        left="#54278f"
        right="#6929c4"
        opacity={opacity}
      />

      <circle cx={node.x - 58} cy={node.y - dataHeight - 34} r="4" fill={status.color} />
      <text
        x={node.x - 48}
        y={node.y - dataHeight - 30}
        fill="#f4f4f4"
        fontSize="12"
        fontFamily="IBM Plex Mono, monospace"
        fontWeight="600"
      >
        {node.id}
      </text>
      {node.isLeader && (
        <g transform={`translate(${node.x + 18} ${node.y - dataHeight - 46})`}>
          <path d="M0 10 2 2l5 5 5-7 5 7 5-5 2 8z" fill="#78a9ff" />
          <rect x="1" y="11" width="22" height="3" fill="#78a9ff" />
        </g>
      )}
      <text
        x={node.x}
        y={node.y - dataHeight + 8}
        textAnchor="middle"
        fill="#d0e2ff"
        fontSize="9"
        fontFamily="IBM Plex Mono, monospace"
      >
        {node.dataShare}% data
      </text>
    </g>
  );
}

export default function TopologyMap({
  nodes,
  selectedId,
  onSelect,
}: {
  nodes: TopologyNode[];
  selectedId: string;
  onSelect: (id: string) => void;
}) {
  const placed = positions(nodes);
  return (
    <div className="overflow-x-auto">
      <svg
        viewBox="0 0 630 410"
        className="w-full min-w-[520px]"
        role="img"
        aria-label="Interactive isometric map of one MeshDB database and its worker nodes"
      >
        <defs>
          <linearGradient id="topology-plane" x1="0" y1="0" x2="1" y2="1">
            <stop offset="0" stopColor="#252525" />
            <stop offset="1" stopColor="#1c1c1c" />
          </linearGradient>
          <filter id="topology-shadow" x="-30%" y="-30%" width="160%" height="180%">
            <feDropShadow dx="0" dy="10" stdDeviation="9" floodColor="#000" floodOpacity="0.42" />
          </filter>
        </defs>

        <polygon points="315,64 590,201 315,354 40,217" fill="url(#topology-plane)" />
        <polygon points="40,217 315,354 315,370 40,233" fill="#111111" />
        <polygon points="315,354 590,201 590,217 315,370" fill="#191919" />
        <g opacity="0.18" stroke="#6f6f6f" strokeWidth="0.7">
          <line x1="109" y1="182" x2="384" y2="319" />
          <line x1="178" y1="148" x2="453" y2="285" />
          <line x1="247" y1="113" x2="522" y2="251" />
          <line x1="109" y1="251" x2="384" y2="98" />
          <line x1="178" y1="285" x2="453" y2="132" />
          <line x1="247" y1="320" x2="522" y2="167" />
        </g>
        <text x="315" y="344" textAnchor="middle" fill="#8d8d8d" fontSize="10" fontFamily="IBM Plex Mono, monospace" letterSpacing="1.5">ONE MESHDB DATABASE</text>

        <g filter="url(#topology-shadow)">
          {placed.map((node) => (
            <NodeStack key={node.id} node={node} selected={node.id === selectedId} onSelect={onSelect} />
          ))}
        </g>
      </svg>
    </div>
  );
}
