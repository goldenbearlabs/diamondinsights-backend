import { ref, uploadBytes } from "firebase/storage";

import { getFirebaseStorage } from "./firebase";

async function toJpegMax1024(file: File): Promise<Blob> {
  const supportsBitmap = typeof window !== "undefined" && "createImageBitmap" in window;
  if (!supportsBitmap) {
    return file;
  }

  const bitmap = await createImageBitmap(file);
  try {
    const targetWidth = bitmap.width > 1024 ? 1024 : bitmap.width;
    const targetHeight = Math.max(1, Math.round((bitmap.height / bitmap.width) * targetWidth));

    const canvas = document.createElement("canvas");
    canvas.width = targetWidth;
    canvas.height = targetHeight;

    const context = canvas.getContext("2d");
    if (!context) {
      return file;
    }
    context.drawImage(bitmap, 0, 0, targetWidth, targetHeight);

    const blob = await new Promise<Blob | null>((resolve) => {
      canvas.toBlob((nextBlob) => resolve(nextBlob), "image/jpeg", 0.85);
    });

    return blob ?? file;
  } finally {
    bitmap.close();
  }
}

export async function uploadProfileImage(file: File, uid: string) {
  const storage = getFirebaseStorage();
  const imageBlob = await toJpegMax1024(file);
  const path = `users/${uid}/profile.jpg`;

  await uploadBytes(ref(storage, path), imageBlob, { contentType: "image/jpeg" });
  return path;
}
