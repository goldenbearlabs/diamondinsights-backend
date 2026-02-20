import { useEffect, useMemo, useState, type ComponentProps } from "react";
import {
  Modal,
  Pressable,
  StyleSheet,
  Text,
  TouchableOpacity,
  View,
  useWindowDimensions,
} from "react-native";
import { usePathname, useRouter } from "expo-router";
import { FontAwesome5 } from "@expo/vector-icons";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { theme } from "../theme/colors";
import { Avatar } from "./Avatar";
import { auth } from "../lib/firebase";

type MenuType = "market" | "gameplay" | null;

type MenuItem = {
  label: string;
  route: string;
  icon: ComponentProps<typeof FontAwesome5>["name"];
};

type MenuKey = Exclude<MenuType, null> | "community";

const MARKET_ROUTES = ["/predictions", "/flipping", "/portfolio", "/leaderboard"];
const GAMEPLAY_ROUTES = [
  "/gameplay-stats",
  "/records",
  "/rankings",
  "/card-comparison",
  "/team-builder",
  "/cards",
];
const COMMUNITY_ROUTES = ["/trending", "/blogs", "/chat"];

const MARKET_ITEMS: MenuItem[] = [
  { label: "Predictions", route: "/(app)/predictions", icon: "chart-line" },
  { label: "Flipping", route: "/(app)/flipping", icon: "exchange-alt" },
  { label: "Portfolio", route: "/(app)/portfolio", icon: "briefcase" },
  { label: "Leaderboard", route: "/(app)/leaderboard", icon: "trophy" },
];

const GAMEPLAY_ITEMS: MenuItem[] = [
  { label: "Stats", route: "/(app)/gameplay-stats", icon: "list-ol" },
  { label: "Records", route: "/(app)/records", icon: "trophy" },
  { label: "Card Comparison", route: "/(app)/card-comparison", icon: "columns" },
  { label: "Team Builder", route: "/(app)/team-builder", icon: "users-cog" },
  { label: "Cards", route: "/(app)/cards", icon: "id-card" },
];

const COMMUNITY_ITEMS: MenuItem[] = [
  { label: "Chat", route: "/(app)/chat", icon: "comment-dots" },
  { label: "Trending", route: "/(app)/trending", icon: "fire-alt" },
  { label: "Blogs", route: "/(app)/blogs", icon: "newspaper" },
];

const MENU_WIDTH = 220;
const BAR_PADDING = 16;
export const BOTTOM_NAV_HEIGHT = 72;
const ACCENT = "#fbbf24";
const ACCENT_DIM = ACCENT;

export const BottomNav = () => {
  const router = useRouter();
  const pathname = usePathname() || "";
  const insets = useSafeAreaInsets();
  const { width } = useWindowDimensions();

  const [menuType, setMenuType] = useState<MenuKey | null>(null);
  const [menuContent, setMenuContent] = useState<MenuKey | null>(null);
  const [marketLayout, setMarketLayout] = useState<{ x: number; width: number } | null>(
    null
  );
  const [gameplayLayout, setGameplayLayout] = useState<{ x: number; width: number } | null>(
    null
  );
  const [communityLayout, setCommunityLayout] = useState<{ x: number; width: number } | null>(
    null
  );
  const currentUid = auth.currentUser?.uid ?? null;
  const profilePath = currentUid ? `users/${currentUid}/profile.jpg` : null;

  useEffect(() => {
    setMenuType(null);
  }, [pathname]);

  useEffect(() => {
    if (menuType) setMenuContent(menuType);
  }, [menuType]);

  const isMarketActive = useMemo(
    () => MARKET_ROUTES.some((route) => pathname.includes(route)),
    [pathname]
  );
  const isGameplayActive = useMemo(
    () => GAMEPLAY_ROUTES.some((route) => pathname.includes(route)),
    [pathname]
  );
  const isCommunityActive = useMemo(
    () => COMMUNITY_ROUTES.some((route) => pathname.includes(route)),
    [pathname]
  );

  const activeMenuType = menuType ?? menuContent;
  const menuItems =
    activeMenuType === "market"
      ? MARKET_ITEMS
      : activeMenuType === "gameplay"
        ? GAMEPLAY_ITEMS
        : activeMenuType === "community"
          ? COMMUNITY_ITEMS
          : [];
          
  const menuLeft = useMemo(() => {
    const minLeft = BAR_PADDING;
    const maxLeft = Math.max(minLeft, width - MENU_WIDTH - minLeft);

    if (activeMenuType === "community" && communityLayout) {
      const rightEdge = BAR_PADDING + communityLayout.x + communityLayout.width;
      const alignedLeft = rightEdge - MENU_WIDTH;
      return Math.min(maxLeft, Math.max(minLeft, alignedLeft));
    }

    if (activeMenuType === "market" && marketLayout) {
      const alignedLeft = BAR_PADDING + marketLayout.x;
      return Math.min(maxLeft, Math.max(minLeft, alignedLeft));
    }

    if (activeMenuType === "gameplay" && gameplayLayout) {
      const alignedLeft = BAR_PADDING + gameplayLayout.x;
      return Math.min(maxLeft, Math.max(minLeft, alignedLeft));
    }

    if (activeMenuType === "market" || activeMenuType === "gameplay") return minLeft;
    return minLeft;
  }, [activeMenuType, width, marketLayout, gameplayLayout, communityLayout]);
  const barPaddingBottom = Math.max(insets.bottom, 10);
  const menuBottom = BOTTOM_NAV_HEIGHT + barPaddingBottom + 16;

  const toggleMenu = (nextMenu: MenuKey) => {
    setMenuType((current) => (current === nextMenu ? null : nextMenu));
  };

  const handleMenuPress = (route: string) => {
    setMenuType(null);
    router.push(route);
  };

  return (
    <View
      style={[
        styles.bar,
        {
          paddingBottom: barPaddingBottom,
          height: BOTTOM_NAV_HEIGHT + barPaddingBottom,
        },
      ]}
    >
      <View style={styles.navGroup}>
        <TouchableOpacity
          style={[
            styles.navButton,
            (isMarketActive || menuType === "market") && styles.navButtonActive,
          ]}
          onPress={() => toggleMenu("market")}
          onLayout={(event) => {
            const { x, width } = event.nativeEvent.layout;
            setMarketLayout({ x, width });
          }}
        >
          <FontAwesome5
            name="chart-line"
            size={18}
            color={
              isMarketActive || menuType === "market"
                ? ACCENT
                : ACCENT_DIM
            }
          />
          <Text
            style={[
              styles.navLabel,
              (isMarketActive || menuType === "market") && styles.navLabelActive,
            ]}
          >
            Market
          </Text>
        </TouchableOpacity>

        <TouchableOpacity
          style={[
            styles.navButton,
            (isGameplayActive || menuType === "gameplay") && styles.navButtonActive,
          ]}
          onPress={() => toggleMenu("gameplay")}
          onLayout={(event) => {
            const { x, width } = event.nativeEvent.layout;
            setGameplayLayout({ x, width });
          }}
        >
          <FontAwesome5
            name="gamepad"
            size={18}
            color={
              isGameplayActive || menuType === "gameplay"
                ? ACCENT
                : ACCENT_DIM
            }
          />
          <Text
            style={[
              styles.navLabel,
              (isGameplayActive || menuType === "gameplay") && styles.navLabelActive,
            ]}
          >
            Gameplay
          </Text>
        </TouchableOpacity>


        <TouchableOpacity
          style={[
            styles.navButton,
            (isCommunityActive || menuType === "community") && styles.navButtonActive,
          ]}
          onPress={() => toggleMenu("community")}
          onLayout={(event) => {
            const { x, width } = event.nativeEvent.layout;
            setCommunityLayout({ x, width });
          }}
        >
          <FontAwesome5
            name="users"
            size={18}
            color={
              isCommunityActive || menuType === "community"
                ? ACCENT
                : ACCENT_DIM
            }
          />
          <Text
            style={[
              styles.navLabel,
              (isCommunityActive || menuType === "community") && styles.navLabelActive,
            ]}
          >
            Community
          </Text>
        </TouchableOpacity>
      </View>

      <TouchableOpacity
        style={styles.profileButton}
        onPress={() => router.push("/(app)/account")}
      >
        <Avatar
          firebasePath={profilePath}
          size={38}
          borderColor="rgba(251, 191, 36, 0.4)"
          borderWidth={1}
        />
      </TouchableOpacity>

      <Modal
        transparent
        visible={menuType !== null}
        animationType="fade"
        onRequestClose={() => setMenuType(null)}
      >
        <View style={styles.menuOverlay}>
          <Pressable style={StyleSheet.absoluteFill} onPress={() => setMenuType(null)} />
          <View style={[styles.menuShell, { paddingBottom: menuBottom }]}>
            <View style={[styles.menuCard, { width: MENU_WIDTH, marginLeft: menuLeft }]}>
              {menuItems.map((item, index) => {
                const mid = (menuItems.length - 1) / 2;
                const curveOffset = Math.abs(mid - index) * 4;

                return (
                <TouchableOpacity
                  key={item.label}
                  style={[
                    styles.menuItem,
                    index === menuItems.length - 1 && styles.menuItemLast,
                    { paddingLeft: 14 + curveOffset },
                  ]}
                  onPress={() => handleMenuPress(item.route)}
                >
                  <FontAwesome5
                    name={item.icon}
                    size={14}
                    color={theme.colors.text}
                    style={styles.menuIcon}
                  />
                  <Text style={styles.menuText}>{item.label}</Text>
                </TouchableOpacity>
                );
              })}
            </View>
          </View>
        </View>
      </Modal>

    </View>
  );
};

const styles = StyleSheet.create({
  bar: {
    paddingHorizontal: BAR_PADDING,
    paddingTop: 8,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    backgroundColor: "#0f172a",
    borderTopColor: "rgba(255,255,255,0.1)",
    borderTopWidth: 1,
  },
  navGroup: {
    flex: 1,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-around",
  },
  navButton: {
    flexDirection: "column",
    alignItems: "center",
    paddingHorizontal: 8,
    paddingVertical: 6,
    borderRadius: 14,
    borderWidth: 1,
    borderColor: "transparent",
  },
  navButtonActive: {
    borderColor: "rgba(251, 191, 36, 0.65)",
    backgroundColor: "rgba(35, 22, 6, 0.7)",
  },
  navLabel: {
    color: ACCENT_DIM,
    fontSize: 9,
    fontWeight: "600",
    letterSpacing: 0.4,
    marginTop: 4,
  },
  navLabelActive: {
    color: ACCENT,
  },
  profileButton: {
  },
  menuOverlay: {
    flex: 1,
    backgroundColor: "rgba(2, 6, 23, 0.4)",
  },
  menuShell: {
    flex: 1,
    justifyContent: "flex-end",
  },
  menuCard: {
    borderRadius: 20,
    backgroundColor: "rgba(15, 23, 42, 0.96)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    overflow: "hidden",
    shadowColor: "#000",
    shadowOpacity: 0.35,
    shadowRadius: 12,
    shadowOffset: { width: 0, height: 8 },
    elevation: 8,
  },
  menuItem: {
    flexDirection: "row",
    alignItems: "center",
    paddingVertical: 12,
    paddingHorizontal: 14,
    borderBottomWidth: 1,
    borderBottomColor: "rgba(148, 163, 184, 0.14)",
  },
  menuItemLast: {
    borderBottomWidth: 0,
  },
  menuIcon: {
    marginRight: 10,
  },
  menuText: {
    color: theme.colors.text,
    fontSize: 14,
    fontWeight: "700",
    letterSpacing: 0.2,
  },
});
