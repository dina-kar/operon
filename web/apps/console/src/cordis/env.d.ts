/// <reference types="vite/client" />

declare module '*.yml?raw' {
  const text: string;
  export default text;
}
