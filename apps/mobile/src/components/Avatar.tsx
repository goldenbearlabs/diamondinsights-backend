import React, { useEffect, useState } from "react";
import { DeviceEventEmitter, StyleSheet, View } from "react-native";
import { Image } from "expo-image";

import { resolveAvatarUrl } from "../lib/profileImage";
import { auth } from "../lib/firebase";
import { theme } from "../theme/colors";

const DEFAULT_PROFILE = require("../../assets/images/default_profile.png");

interface AvatarProps {
  firebasePath: string | null | undefined;
  size: number;
  borderColor?: string;
  borderWidth?: number;
}

export function Avatar({
  firebasePath,
  size,
  borderColor,
  borderWidth = 0,
}: AvatarProps) {
  const [uri, setUri] = useState<string | null>(null);

  useEffect(() => {
    let active = true;

    if (!firebasePath) {
      setUri(null);
      return;
    }

    resolveAvatarUrl(firebasePath).then((url) => {
      if (active) setUri(url);
    });

    return () => {
      active = false;
    };
  }, [firebasePath]);

  useEffect(() => {
    const currentUid = auth.currentUser?.uid;
    if (!currentUid || !firebasePath) return;

    const isCurrentUserProfile = firebasePath === `users/${currentUid}/profile.jpg`;
    if (!isCurrentUserProfile) return;

    const handleProfileUpdate = () => {
      setUri(null);
      resolveAvatarUrl(firebasePath).then((url) => {
        setUri(url);
      });
    };

    const subscription = DeviceEventEmitter.addListener(
      "profile-image-updated",
      handleProfileUpdate
    );

    return () => {
      subscription.remove();
    };
  }, [firebasePath]);

  const outerSize = size + borderWidth * 2;

  return (
    <View
      style={[
        styles.frame,
        {
          width: outerSize,
          height: outerSize,
          borderRadius: outerSize / 2,
          borderWidth,
          borderColor: borderColor ?? "transparent",
        },
      ]}
    >
      <Image
        source={uri ? { uri } : DEFAULT_PROFILE}
        style={{
          width: size,
          height: size,
          borderRadius: size / 2,
        }}
        cachePolicy="memory-disk"
        transition={200}
        placeholder={DEFAULT_PROFILE}
        onError={() => setUri(null)}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  frame: {
    overflow: "hidden",
    backgroundColor: theme.colors.border,
  },
});
