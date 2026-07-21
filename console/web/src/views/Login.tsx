import { useState } from "react";
import { useAuth } from "../auth";
import { Button, TextInput, Banner } from "../components/ui";

export default function Login() {
  const { signIn } = useAuth();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setBusy(true);
    try {
      await signIn(username, password);
    } catch (err) {
      setError(err instanceof Error ? err.message : "login failed");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="h-full grid place-items-center bg-carbon-bg">
      <form onSubmit={submit} className="w-80 bg-carbon-layer border border-carbon-border p-6 space-y-4">
        <div>
          <div className="text-carbon-text text-lg font-semibold">meshdb console</div>
          <div className="text-carbon-text-3 text-xs">Sign in to manage your clusters</div>
        </div>
        {error && <Banner tone="error">{error}</Banner>}
        <TextInput
          label="Username"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          autoFocus
        />
        <TextInput
          label="Password"
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
        />
        <Button type="submit" className="w-full" disabled={busy}>
          {busy ? "Signing in…" : "Sign in"}
        </Button>
      </form>
    </div>
  );
}
