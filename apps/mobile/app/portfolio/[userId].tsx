import { useLocalSearchParams, useRouter } from 'expo-router';
import { useEffect, useState } from 'react';
import { ActivityIndicator, View } from 'react-native';
import { apiGetAuth } from '../../src/lib/api';
import { auth } from '../../src/lib/firebase';
import PublicPortfolioScreen from '../../src/screens/portfolio/PublicPortfolioScreen';
import { theme } from '../../src/theme/colors';

type UserProfile = {
  id: number;
  firebase_id?: string | null;
  display_name: string;
  is_me: boolean;
};

export default function PublicPortfolioRoute() {
  const params = useLocalSearchParams<{ userId: string; username?: string }>();
  const router = useRouter();
  const userId = params.userId;
  const [username, setUsername] = useState(params.username ?? '');
  const [checking, setChecking] = useState(true);

  useEffect(() => {
    const check = async () => {
      try {
        const profile = await apiGetAuth<UserProfile>(`/users/${userId}`);

        if (profile.is_me) {
          router.replace('/(app)/portfolio');
          return;
        }

        if (!params.username) {
          setUsername(profile.display_name);
        }
      } catch {
      } finally {
        setChecking(false);
      }
    };

    check();
  }, [userId]);

  if (checking) {
    return (
      <View style={{ flex: 1, justifyContent: 'center', alignItems: 'center', backgroundColor: theme.colors.background }}>
        <ActivityIndicator size="large" color="#fbbf24" />
      </View>
    );
  }

  return <PublicPortfolioScreen userId={userId} username={username} />;
}
