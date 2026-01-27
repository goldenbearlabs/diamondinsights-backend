import type { User } from "firebase/auth";
import { apiGetAuth, apiPostAuth, ApiError } from "./api";

type UserOut = {
  id: number;
  firebase_id: string;
  email: string;
  display_name: string;
  profile_img_path: string;
};

export async function ensureBackendUser(
  fbUser: User,
  opts?: { displayName?: string; profileImgPath?: string }
) {
  console.warn("[ensureBackendUser] uid", fbUser.uid);

  try {
    console.warn("[ensureBackendUser] GET /users/me");
    return await apiGetAuth<UserOut>("/users/me");
  } catch (e) {
    if (e instanceof ApiError && e.status === 404) {
      console.warn("[ensureBackendUser] 404 -> POST /users/signup");

      const displayName =
        opts?.displayName?.trim() ||
        fbUser.displayName?.trim() ||
        (fbUser.email ? fbUser.email.split("@")[0] : "User");

      const profileImgPath = opts?.profileImgPath || `users/${fbUser.uid}/profile.jpg`;

      return await apiPostAuth<UserOut>("/users/signup", {
        display_name: displayName,
        profile_img_path: profileImgPath,
      });
    }
    throw e;
  }
}