import { useState } from "react";
import {
  View,
  Text,
  TextInput,
  Pressable,
  Image,
  ActivityIndicator,
  StyleSheet,
  ScrollView,
} from "react-native";
import * as ImagePicker from "expo-image-picker";
import { createUserWithEmailAndPassword, deleteUser, updateProfile, type User } from "firebase/auth";
import { useRouter } from "expo-router";

import { ApiError, getDisplayNameAvailability } from "../lib/api";
import { toReadableAuthError } from "../lib/authErrors";
import { auth } from "../lib/firebase";
import { uploadProfileImage } from "../lib/storage";
import { ensureBackendUser } from "../lib/userSync";
import { theme } from "../theme/colors";

export default function SignUpScreen() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [photoUri, setPhotoUri] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const pickImage = async () => {
    setError(null);
    const perm = await ImagePicker.requestMediaLibraryPermissionsAsync();
    if (!perm.granted) {
      setError("Photo permission denied.");
      return;
    }
    const result = await ImagePicker.launchImageLibraryAsync({
        mediaTypes: ["images"],
        allowsEditing: true,
        aspect: [1, 1],
        quality: 0.85,
    });
    if (!result.canceled) setPhotoUri(result.assets[0].uri);
  };

  const onSignUp = async () => {
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

      const cred = await createUserWithEmailAndPassword(auth, email.trim(), password);
      const user = cred.user;
      createdUser = user;

      await updateProfile(user, { displayName: normalizedDisplayName });

      let profileImgPath: string | undefined;
      if (photoUri) {
        profileImgPath = await uploadProfileImage(photoUri, user.uid);
      }

      await ensureBackendUser(user, { displayName: normalizedDisplayName, profileImgPath });
      router.replace("/(app)");
    } catch (e: unknown) {
      if (createdUser && e instanceof ApiError && e.status === 409) {
        try {
          await deleteUser(createdUser);
        } catch {
          // Best effort rollback if backend rejected account creation.
        }
      }
      setError(toReadableAuthError(e, "Signup failed"));
    } finally {
      setLoading(false);
    }
  };


  return (
    <ScrollView
      contentContainerStyle={styles.page}
      keyboardShouldPersistTaps="handled"
    >
      <View style={styles.card}>
        <Text style={styles.title}>Create your account</Text>
        <Text style={styles.subtitle}>Start tracking live market insights</Text>

        <View style={styles.fieldGroup}>
          <Text style={styles.label}>Display name</Text>
          <TextInput
            placeholder="Your name"
            placeholderTextColor={theme.colors.muted}
            autoCapitalize="words"
            value={displayName}
            onChangeText={setDisplayName}
            style={styles.input}
          />
        </View>

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

        <Pressable onPress={pickImage} style={styles.secondaryButton}>
          <Text style={styles.secondaryButtonText}>
            {photoUri ? "Change profile photo" : "Pick profile photo (optional)"}
          </Text>
        </Pressable>

        {photoUri ? (
          <Image source={{ uri: photoUri }} style={styles.avatarPreview} />
        ) : null}

        {error ? <Text style={styles.errorText}>{error}</Text> : null}

        <Pressable
          onPress={onSignUp}
          disabled={loading}
          style={[styles.primaryButton, loading && styles.buttonDisabled]}
        >
          {loading ? (
            <ActivityIndicator color="white" />
          ) : (
            <Text style={styles.primaryButtonText}>Sign up</Text>
          )}
        </Pressable>

        <View style={styles.switchRow}>
          <Text style={styles.switchText}>Already have an account?</Text>
          <Pressable onPress={() => router.replace("/(auth)/signin")}>
            <Text style={styles.switchLink}>Sign in</Text>
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
  secondaryButton: {
    borderWidth: 1,
    borderColor: "rgba(255, 255, 255, 0.12)",
    backgroundColor: "rgba(15, 23, 42, 0.4)",
    borderRadius: 12,
    paddingVertical: 12,
    paddingHorizontal: 14,
  },
  secondaryButtonText: {
    color: theme.colors.text,
    fontSize: 14,
    fontWeight: "600",
  },
  avatarPreview: {
    width: 88,
    height: 88,
    borderRadius: 44,
    borderWidth: 2,
    borderColor: theme.colors.primary,
    alignSelf: "center",
  },
  errorText: {
    color: theme.colors.error,
    fontSize: 13,
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
