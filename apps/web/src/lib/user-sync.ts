import type { User } from "firebase/auth";

import { ApiError, apiGetAuth, apiPostAuth } from "./api";

type UserOut = {
  id: number;
  firebase_id: string;
  email: string;
  display_name: string;
  profile_img_path: string;
};

export async function ensureBackendUser(
  fbUser: User,
  opts?: { displayName?: string; profileImgPath?: string },
) {
  try {
    return await apiGetAuth<UserOut>("/users/me");
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) {
      const displayName =
        opts?.displayName?.trim() ||
        fbUser.displayName?.trim() ||
        (fbUser.email ? fbUser.email.split("@")[0] : "User");

      const profileImgPath = opts?.profileImgPath || `users/${fbUser.uid}/profile.jpg`;

      return apiPostAuth<UserOut>("/users/signup", {
        display_name: displayName,
        profile_img_path: profileImgPath,
      });
    }
    throw error;
  }
}
