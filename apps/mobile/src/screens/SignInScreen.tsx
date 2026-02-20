import { useState } from "react";
import {
  View,
  Text,
  TextInput,
  Pressable,
  ActivityIndicator,
  StyleSheet,
  ScrollView,
} from "react-native";
import { sendPasswordResetEmail, signInWithEmailAndPassword } from "firebase/auth";
import { useRouter } from "expo-router";

import { auth } from "../lib/firebase";
import { ensureBackendUser } from "../lib/userSync";
import { toReadableAuthError } from "../lib/authErrors";
import { theme } from "../theme/colors";

export default function SignInScreen() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [resettingPassword, setResettingPassword] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const onSignIn = async () => {
    setError(null);
    setNotice(null);
    setLoading(true);
    try {
      const cred = await signInWithEmailAndPassword(auth, email.trim(), password);
      await ensureBackendUser(cred.user);
      router.replace("/(app)");
    } catch (e: unknown) {
      setError(toReadableAuthError(e, "Sign in failed"));
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
      await sendPasswordResetEmail(auth, normalizedEmail);
      setNotice("Password reset email sent.");
    } catch (e: unknown) {
      setError(toReadableAuthError(e, "Failed to send reset email"));
    } finally {
      setResettingPassword(false);
    }
  };

  return (
    <ScrollView
      contentContainerStyle={styles.page}
      keyboardShouldPersistTaps="handled"
    >
      <View style={styles.card}>
        <Text style={styles.title}>Welcome back</Text>
        <Text style={styles.subtitle}>Sign in to continue</Text>

        <View style={styles.fieldGroup}>
          <Text style={styles.label}>Email</Text>
          <TextInput
            placeholder="you@email.com"
            placeholderTextColor={theme.colors.muted}
            autoCapitalize="none"
            keyboardType="email-address"
            value={email}
            onChangeText={setEmail}
            style={styles.input}
          />
        </View>

        <View style={styles.fieldGroup}>
          <Text style={styles.label}>Password</Text>
          <TextInput
            placeholder="Password"
            placeholderTextColor={theme.colors.muted}
            secureTextEntry
            value={password}
            onChangeText={setPassword}
            style={styles.input}
          />
        </View>

        <View style={styles.forgotRow}>
          <Pressable
            onPress={onForgotPassword}
            disabled={loading || resettingPassword}
          >
            <Text style={styles.forgotLink}>
              {resettingPassword ? "Sending reset..." : "Forgot password?"}
            </Text>
          </Pressable>
        </View>

        {error ? <Text style={styles.errorText}>{error}</Text> : null}
        {notice ? <Text style={styles.noticeText}>{notice}</Text> : null}

        <Pressable
          onPress={onSignIn}
          disabled={loading || resettingPassword}
          style={[styles.primaryButton, loading && styles.buttonDisabled]}
        >
          {loading ? (
            <ActivityIndicator color="white" />
          ) : (
            <Text style={styles.primaryButtonText}>Sign in</Text>
          )}
        </Pressable>

        <View style={styles.switchRow}>
          <Text style={styles.switchText}>New here?</Text>
          <Pressable onPress={() => router.replace("/(auth)/signup")}>
            <Text style={styles.switchLink}>Create account</Text>
          </Pressable>
        </View>
      </View>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  page: {
    flexGrow: 1,
    backgroundColor: theme.colors.background,
    padding: theme.spacing.l,
    justifyContent: "center",
  },
  card: {
    backgroundColor: theme.colors.card,
    borderRadius: 20,
    padding: theme.spacing.l,
    borderWidth: 1,
    borderColor: "rgba(255, 255, 255, 0.08)",
    gap: 12,
  },
  title: {
    color: theme.colors.text,
    fontSize: 26,
    fontWeight: "700",
  },
  subtitle: {
    color: theme.colors.muted,
    fontSize: 14,
    marginBottom: 8,
  },
  fieldGroup: {
    gap: 8,
  },
  label: {
    color: theme.colors.muted,
    fontSize: 12,
    fontWeight: "700",
    letterSpacing: 0.6,
    textTransform: "uppercase",
  },
  input: {
    borderWidth: 1,
    borderColor: "rgba(255, 255, 255, 0.12)",
    backgroundColor: "rgba(15, 23, 42, 0.6)",
    color: theme.colors.text,
    paddingVertical: 12,
    paddingHorizontal: 14,
    borderRadius: 12,
    fontSize: 16,
  },
  errorText: {
    color: theme.colors.error,
    fontSize: 13,
  },
  noticeText: {
    color: theme.colors.primary,
    fontSize: 13,
  },
  forgotRow: {
    flexDirection: "row",
    justifyContent: "flex-end",
  },
  forgotLink: {
    color: "#fbbf24",
    fontSize: 13,
    fontWeight: "700",
  },
  primaryButton: {
    marginTop: 4,
    backgroundColor: theme.colors.primary,
    paddingVertical: 14,
    borderRadius: 12,
    alignItems: "center",
  },
  primaryButtonText: {
    color: "white",
    fontSize: 16,
    fontWeight: "700",
  },
  buttonDisabled: {
    opacity: 0.7,
  },
  switchRow: {
    marginTop: 6,
    flexDirection: "row",
    justifyContent: "center",
    gap: 6,
  },
  switchText: {
    color: theme.colors.muted,
    fontSize: 13,
  },
  switchLink: {
    color: "#fbbf24",
    fontSize: 13,
    fontWeight: "700",
  },
});
