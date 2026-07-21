import { createContext, useContext, useEffect, useState, ReactNode } from "react";
import * as api from "./lib/api";

interface AuthState {
  me: api.Me | null;
  loading: boolean;
  refresh: () => Promise<void>;
  signIn: (u: string, p: string) => Promise<void>;
  signOut: () => Promise<void>;
}

const Ctx = createContext<AuthState | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [me, setMe] = useState<api.Me | null>(null);
  const [loading, setLoading] = useState(true);

  const refresh = async () => {
    try {
      setMe(await api.me());
    } catch {
      setMe(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refresh();
  }, []);

  const signIn = async (u: string, p: string) => {
    setMe(await api.login(u, p));
  };
  const signOut = async () => {
    await api.logout();
    setMe(null);
  };

  return <Ctx.Provider value={{ me, loading, refresh, signIn, signOut }}>{children}</Ctx.Provider>;
}

export function useAuth() {
  const ctx = useContext(Ctx);
  if (!ctx) throw new Error("useAuth outside AuthProvider");
  return ctx;
}
