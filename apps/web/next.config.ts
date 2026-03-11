import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  images: {
    remotePatterns: [
      {
        protocol: "https",
        hostname: "cards.theshow.com",
      },
    ],
  },
};

export default nextConfig;
