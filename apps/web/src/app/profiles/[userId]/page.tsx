import PublicProfilePage from "./PublicProfilePage";

export default async function ProfilePage({
  params,
}: {
  params: Promise<{ userId: string }>;
}) {
  const { userId } = await params;
  return <PublicProfilePage userId={userId} />;
}
