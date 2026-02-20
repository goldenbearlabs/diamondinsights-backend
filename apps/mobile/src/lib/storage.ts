import { ref, uploadBytes } from "firebase/storage";
import { storage } from "./firebase";
import * as ImageManipulator from "expo-image-manipulator";

async function toJpegMax1024(uri: string) {
  const ctx = ImageManipulator.ImageManipulator.manipulate(uri);

  const first = await ctx.renderAsync();
  if (first.width > 1024) {
    ctx.resize({ width: 1024, height: null });
  }

  const finalRef = await ctx.renderAsync();
  const saved = await finalRef.saveAsync({
    format: ImageManipulator.SaveFormat.JPEG,
    compress: 0.85,
  });

  return saved.uri;
}

export async function uploadProfileImage(uri: string, uid: string) {
  const jpegUri = await toJpegMax1024(uri);

  const res = await fetch(jpegUri);
  const blob = await res.blob();

  const path = `users/${uid}/profile.jpg`;
  await uploadBytes(ref(storage, path), blob, { contentType: "image/jpeg" });

  return path;
}
