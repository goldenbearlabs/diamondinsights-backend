"use client";

import Image from "next/image";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { ChangeEvent, FormEvent, useEffect, useState } from "react";
import { createUserWithEmailAndPassword, deleteUser, updateProfile, type User } from "firebase/auth";

import styles from "../auth.module.css";
import { ApiError, getDisplayNameAvailability } from "@/lib/api";
import { toReadableAuthError } from "@/lib/auth-errors";
import { getFirebaseAuth } from "@/lib/firebase";
import { uploadProfileImage } from "@/lib/storage";
import { ensureBackendUser } from "@/lib/user-sync";

export default function SignUpPage() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [photoFile, setPhotoFile] = useState<File | null>(null);
  const [photoPreviewUrl, setPhotoPreviewUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!photoFile) {
      setPhotoPreviewUrl(null);
      return;
    }
    const nextUrl = URL.createObjectURL(photoFile);
    setPhotoPreviewUrl(nextUrl);

    return () => {
      URL.revokeObjectURL(nextUrl);
    };
  }, [photoFile]);

  const onFileChange = (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0] ?? null;
    setPhotoFile(file);
  };

  const onSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const normalizedDisplayName = displayName.trim();
    if (!normalizedDisplayName) {
      setError("Username is required.");
      return;
    }

    setError(null);
    setLoading(true);

    let createdUser: User | null = null;

    try {
      const availability = await getDisplayNameAvailability(normalizedDisplayName);
      if (!availability.available) {
        setError("That username is already taken. Try another one.");
        return;
      }

      const auth = getFirebaseAuth();
      const credential = await createUserWithEmailAndPassword(auth, email.trim(), password);
      const user = credential.user;
      createdUser = user;

      await updateProfile(user, { displayName: normalizedDisplayName });

      let profileImgPath: string | undefined;
      if (photoFile) {
        profileImgPath = await uploadProfileImage(photoFile, user.uid);
      }

      await ensureBackendUser(user, {
        displayName: normalizedDisplayName,
        profileImgPath,
      });
      router.replace("/");
    } catch (err: unknown) {
      if (createdUser && err instanceof ApiError && err.status === 409) {
        try {
          await deleteUser(createdUser);
        } catch {
          // Best effort rollback if backend rejected account creation.
        }
      }
      setError(toReadableAuthError(err, "Signup failed"));
    } finally {
      setLoading(false);
    }
  };

  return (
    <main className={styles.page}>
      <section className={styles.card}>
        <h1 className={styles.title}>Create your account</h1>
        <p className={styles.subtitle}>Start tracking live market insights</p>

        <form className={styles.form} onSubmit={onSubmit}>
          <label className={styles.field}>
            <span className={styles.label}>Display name</span>
            <input
              className={styles.input}
              type="text"
              placeholder="Your name"
              autoComplete="nickname"
              value={displayName}
              onChange={(event) => setDisplayName(event.target.value)}
            />
          </label>

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
              autoComplete="new-password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
            />
          </label>

          <label className={styles.secondaryButton}>
            {photoFile ? "Change profile photo" : "Pick profile photo (optional)"}
            <input type="file" accept="image/*" onChange={onFileChange} hidden />
          </label>

          {photoPreviewUrl ? (
            <Image
              src={photoPreviewUrl}
              alt="Profile preview"
              width={88}
              height={88}
              className={styles.avatarPreview}
              unoptimized
            />
          ) : null}

          {error ? <p className={styles.messageError}>{error}</p> : null}

          <button type="submit" className={styles.primaryButton} disabled={loading}>
            {loading ? "Creating account..." : "Sign up"}
          </button>
        </form>

        <p className={styles.switchRow}>
          <span>Already have an account?</span>
          <Link href="/signin">Sign in</Link>
        </p>

        <Link href="/" className={styles.homeLink}>
          Back to home
        </Link>
      </section>
    </main>
  );
}
