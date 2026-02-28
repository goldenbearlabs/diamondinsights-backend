"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { FormEvent, useState } from "react";
import { sendPasswordResetEmail, signInWithEmailAndPassword } from "firebase/auth";

import styles from "../auth.module.css";
import { toReadableAuthError } from "@/lib/auth-errors";
import { getFirebaseAuth } from "@/lib/firebase";
import { ensureBackendUser } from "@/lib/user-sync";

export default function SignInPage() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [resettingPassword, setResettingPassword] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const onSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError(null);
    setNotice(null);
    setLoading(true);
    try {
      const auth = getFirebaseAuth();
      const credential = await signInWithEmailAndPassword(auth, email.trim(), password);
      await ensureBackendUser(credential.user);
      router.replace("/");
    } catch (err: unknown) {
      setError(toReadableAuthError(err, "Sign in failed"));
    } finally {
      setLoading(false);
    }
  };

  const onForgotPassword = async () => {
    const normalizedEmail = email.trim();
    if (!normalizedEmail) {
      setNotice(null);
      setError("Enter your email address to reset your password.");
      return;
    }

    setError(null);
    setNotice(null);
    setResettingPassword(true);
    try {
      const auth = getFirebaseAuth();
      await sendPasswordResetEmail(auth, normalizedEmail);
      setNotice("Password reset email sent.");
    } catch (err: unknown) {
      setError(toReadableAuthError(err, "Failed to send reset email"));
    } finally {
      setResettingPassword(false);
    }
  };

  return (
    <main className={styles.page}>
      <section className={styles.card}>
        <h1 className={styles.title}>Welcome back</h1>
        <p className={styles.subtitle}>Sign in to continue</p>

        <form className={styles.form} onSubmit={onSubmit}>
          <label className={styles.field}>
            <span className={styles.label}>Email</span>
            <input
              className={styles.input}
              type="email"
              placeholder="you@email.com"
              autoComplete="email"
              value={email}
              onChange={(event) => setEmail(event.target.value)}
            />
          </label>

          <label className={styles.field}>
            <span className={styles.label}>Password</span>
            <input
              className={styles.input}
              type="password"
              placeholder="Password"
              autoComplete="current-password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
            />
          </label>

          <div className={styles.forgotRow}>
            <button
              type="button"
              className={styles.forgotButton}
              onClick={onForgotPassword}
              disabled={loading || resettingPassword}
            >
              {resettingPassword ? "Sending reset..." : "Forgot password?"}
            </button>
          </div>

          {error ? <p className={styles.messageError}>{error}</p> : null}
          {notice ? <p className={styles.messageNotice}>{notice}</p> : null}

          <button type="submit" className={styles.primaryButton} disabled={loading || resettingPassword}>
            {loading ? "Signing in..." : "Sign in"}
          </button>
        </form>

        <p className={styles.switchRow}>
          <span>New here?</span>
          <Link href="/signup">Create account</Link>
        </p>

        <Link href="/" className={styles.homeLink}>
          Back to home
        </Link>
      </section>
    </main>
  );
}
