import { useEffect, useMemo, useState } from "react";
import { Image, StyleSheet, Text, View } from "react-native";
import { apiGet } from "../src/lib/api";
import { Screen } from "../src/components/Screen";
import { ThemedText } from "../src/components/ThemedText";
import { useTheme } from "../src/theme/ThemeProvider";
import { Button } from "../src/components/Button";

type Card = {
  id: string;
  name: string;
  img: string;
};

export default function Home() {
  const theme = useTheme();
  const styles = useMemo(
    () =>
      StyleSheet.create({
        screen: {
          padding: 0,
        },
        content: {
          padding: theme.spacing.lg,
          paddingTop: theme.spacing.xl,
        },
        background: {
          ...StyleSheet.absoluteFillObject,
          backgroundColor: theme.colors.background,
        },
        spot: {
          position: "absolute",
          borderRadius: 999,
        },
        titleRow: {
          flexDirection: "row",
          alignItems: "center",
          marginBottom: theme.spacing.lg,
        },
        logo: {
          width: 100,
          height: 100,
          borderRadius: theme.radius.sm,
          marginRight: theme.spacing.sm,
          marginLeft: -theme.spacing.md,
        },
        title: {
          fontSize: theme.typography.sizes.xxl,
          lineHeight: theme.typography.lineHeights.xxl,
        },
        titleDiamond: {
          color: theme.colors.text,
        },
        titleInsights: {
          color: theme.colors.primary,
        },
        subheader: {
          color: theme.colors.muted,
          marginBottom: theme.spacing.lg,
        },
        ctaRow: {
          marginTop: theme.spacing.lg,
        },
        ctaPrimary: {
          marginBottom: theme.spacing.sm,
        },
        cardPanel: {
          marginTop: theme.spacing.xl,
          padding: theme.spacing.lg,
          backgroundColor: theme.colors.surface,
          borderRadius: theme.radius.lg,
          borderWidth: 1,
          borderColor: theme.colors.border,
        },
        cardImage: {
          marginTop: theme.spacing.md,
          width: 220,
          height: 320,
          borderRadius: theme.radius.md,
          alignSelf: "center",
        },
        error: {
          marginTop: theme.spacing.md,
          color: theme.colors.danger,
        },
        stubImage: {
            width: 20,
            height: 20,
            marginRight: theme.spacing.sm,
        }
      }),
    [theme],
  );
  const [card, setCard] = useState<Card | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let isMounted = true;
    apiGet<Card[]>("/cards?limit=1")
      .then((cards) => {
        if (!isMounted) return;
        setCard(cards[0] ?? null);
      })
      .catch((err: Error) => {
        if (!isMounted) return;
        setError(err.message);
      });
    return () => {
      isMounted = false;
    };
  }, []);

  return (
    <Screen style={styles.screen}>
      <View pointerEvents="none" style={styles.background}>
        {[
          { size: 240, top: -80, left: -60, color: "rgba(11, 77, 162, 0.25)" },
          { size: 180, top: 80, right: -40, color: "rgba(28, 40, 58, 0.35)" },
          { size: 140, bottom: 40, left: 30, color: "rgba(11, 77, 162, 0.18)" },
          { size: 220, bottom: -80, right: -30, color: "rgba(16, 26, 40, 0.5)" },
        ].map((spot, index) => (
          <View
            key={`spot-${index}`}
            style={[
              styles.spot,
              {
                width: spot.size,
                height: spot.size,
                backgroundColor: spot.color,
                top: spot.top,
                left: spot.left,
                right: spot.right,
                bottom: spot.bottom,
              },
            ]}
          />
        ))}
      </View>
      <View style={styles.content}>
        <View style={styles.titleRow}>
          <Image
            source={require("../assets/images/placeholder.png")}
            style={styles.logo}
            resizeMode="cover"
          />
          <ThemedText variant="title" style={styles.title}>
            <Text style={styles.titleDiamond}>Diamond{"\n"}</Text>
            <Text style={styles.titleInsights}>Insights</Text>
          </ThemedText>
        </View>

        <ThemedText variant="subtitle" style={styles.subheader}>
          View Cards, Track Investments, & Improve Your Game.
        </ThemedText>

        <View style={styles.ctaRow}>
          <Button style={styles.ctaPrimary} onPress={() => {}}>
            <Image
                source={require("../assets/images/stub.png")}
                style={styles.stubImage}
            />
            Get Started
          </Button>
          <Button variant="ghost" onPress={() => {}}>
            Explore Cards
          </Button>
        </View>
        <View style={styles.cardPanel}>
          {error ? (
            <ThemedText style={styles.error}>{error}</ThemedText>
          ) : card ? (
            <>
              <ThemedText variant="subtitle" style={{ marginTop: theme.spacing.md }}>
                {card.name}
              </ThemedText>
              <Image
                source={{ uri: card.img }}
                style={styles.cardImage}
                resizeMode="cover"
              />
            </>
          ) : (
            <ThemedText style={{ marginTop: theme.spacing.md }}>
              Loading card...
            </ThemedText>
          )}
        </View>
      </View>
    </Screen>
  );
}
