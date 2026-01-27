import { Image, StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams } from "expo-router";

export default function CardScreen() {
  const params = useLocalSearchParams<{
    cardId?: string | string[];
    cardName?: string | string[];
    cardYear?: string | string[];
    cardOvr?: string | string[];
    cardImg?: string | string[];
  }>();

  const cardName = Array.isArray(params.cardName) ? params.cardName[0] : params.cardName;
  const cardYear = Array.isArray(params.cardYear) ? params.cardYear[0] : params.cardYear;
  const cardOvr = Array.isArray(params.cardOvr) ? params.cardOvr[0] : params.cardOvr;
  const cardImg = Array.isArray(params.cardImg) ? params.cardImg[0] : params.cardImg;

  return (
    <View style={styles.container}>
      {cardImg ? <Image source={{ uri: cardImg }} style={styles.image} /> : null}
      <Text style={styles.title}>{cardName || "Card"}</Text>
      <Text style={styles.meta}>
        {cardYear ? `${cardYear}` : "Year"} · {cardOvr ? `OVR ${cardOvr}` : "OVR"}
      </Text>
      <Text style={styles.note}>Card details coming soon.</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#020617",
    alignItems: "center",
    justifyContent: "center",
    paddingHorizontal: 24,
    gap: 8,
  },
  image: {
    width: 110,
    height: 110,
    borderRadius: 16,
    marginBottom: 12,
  },
  title: {
    color: "white",
    fontSize: 20,
    fontWeight: "700",
    textAlign: "center",
  },
  meta: {
    color: "rgba(255,255,255,0.7)",
    fontSize: 13,
  },
  note: {
    color: "rgba(255,255,255,0.6)",
    fontSize: 12,
    marginTop: 6,
  },
});
