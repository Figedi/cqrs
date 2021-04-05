const isProduction = process.env.NODE_ENV === "production";
const shouldLog = process.env.SHOULD_LOG ? process.env.SHOULD_LOG === "true" : false;

module.exports = {
  name: "default",
  synchronize: true,
  migrationsRun: true,
  cache: true,
  logging: shouldLog,
  entities: [isProduction ? "dist/**/*Entity.js" : "src/**/*Entity.ts"],
  migrations: [isProduction ? "dist/migrations/*.js" : "src/migrations/*.ts"],
  subscribers: [isProduction ? "dist/subscribers/*.js" : "src/subscribers/*.ts"],
  cli: {
    entitiesDir: isProduction ? "./dist/entity" : "./src/entity",
    migrationsDir: isProduction ? "./dist/migrations" : "./src/migrations",
  },
  type: "postgres",
  url: `postgres://${process.env.PG_USER_NAME}:${process.env.PG_PASSWORD}@${process.env.PG_HOST}:5432/${process.env.PG_DB}`,
  extra: { max: isProduction ? 10 : 50 }
};
