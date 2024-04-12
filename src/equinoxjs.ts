import { DynamoDB } from "@aws-sdk/client-dynamodb";
import { Decider, StreamId, StreamName, Uuid, Codec, CachingStrategy, ICodec } from "@equinox-js/core"
import { DynamoStoreContext, AccessStrategy, DynamoStoreCategory, DynamoStoreClient, TipOptions, QueryOptions } from '@equinox-js/dynamo-store';

export type AccountId = Uuid.Uuid<"AccountId">
export const AccountId = Uuid.create<"AccountId">()

export namespace Stream {
  export const category = "Account"
  export const id = StreamId.gen(AccountId.toString)
  export const decodeId = StreamId.dec(AccountId.parse)
  export const match = StreamName.tryMatch(category, decodeId)
}

export namespace Events {
  export type Amount = { amount: number }
  export type Event = 
    | { type: 'Opened'; data: { status: Fold.AccountStatus; name:string; amount: number; } }
    | { type: "Deposited"; data: { amount: number; } }
    | { type: "Withdrawn"; data: { amount: number; } }
  export const codec: ICodec<Event, string> = {
    decode(event): Event | undefined {
        const data = JSON.parse(event.data || "{}");
        switch (event.type) {
            case "Opened": return { type: event.type, data: { status: data.status, name: data.name, amount: data.amount } }
            case "Deposited": return { type: event.type, data: { amount: data.amount } }
            case "Withdrawn": return { type: event.type, data: { amount: data.amount } }
        }
    }, 
    encode(event) {
        const data = "data" in event ? JSON.stringify(event.data) : undefined;
        return { type: event.type, data }
    } 
  } 
}

export namespace Fold {
  import Event = Events.Event

  export type AccountStatus = "NonExistent" | "Open" | "Closed";

  export type State = {
    status: AccountStatus;
    name: string;
    amount: number;
  }

  export const initial: State = { status: 'NonExistent', name: "", amount: 0 }

  export const evolve = (state: State, event: Event): State => {
    switch (event.type) {
      case "Opened":
        return {
          ...state,
          status: "Open",
          name: event.data.name
        };
      case "Deposited":
        return {
          ...state,
          amount: state.amount + event.data.amount
        };
      case "Withdrawn":
        return {
          ...state,
          amount: state.amount - event.data.amount
        }
    }
  }
  export const fold = (state: State, events: Event[]) => events.reduce(evolve, state)
}

export namespace Decide {
  import Event = Events.Event
  import State = Fold.State

  export const open =
    (accountId: AccountId, name: string) =>
    (state: State): Event[] => {
      return [{ type: "Opened", data: { status: 'Open', name, amount: 0 } }]
    }

  export const deposit =
    (amount: number) =>
    (state: State): Event[] => {
      if (state.status !== "Open") throw new Error("Account is not open");
      return [{ type: "Deposited", data: { amount } }]
    }
  export const withdraw =
    (amount: number) =>
    (state: State): Event[] => {
      if (state.status !== "Open") throw new Error("Account is not open");
      if (state.amount < amount) throw new Error("Insufficient funds")
      return [{ type: "Withdrawn", data: { amount } }]
    }
}

type resolveType = (accountId: AccountId) => Decider<Events.Event, Fold.State>;

export class Service {
    constructor(private readonly resolve: resolveType) {}

    open(accountId: AccountId, name: string) {
        const decider = this.resolve(accountId)
        return decider.transact(Decide.open(accountId, name))
    }

    deposit(accountId: AccountId, amount: number) {
        const decider = this.resolve(accountId)
        return decider.transact(Decide.deposit(amount))
    }

    withdraw(accountId: AccountId, amount: number) {
        const decider = this.resolve(accountId)
        return decider.transact(Decide.withdraw(amount))
    }

    readBalance(accountId: AccountId) {
        const decider = this.resolve(accountId)
        return decider.query((state) => state)
    }

    static create(accountId: AccountId, context: DynamoStoreContext) {
        const access = AccessStrategy.Unoptimized();
        // prettier-ignore
        const category = DynamoStoreCategory.create(context, Stream.category, Codec.compress(Events.codec), Fold.fold, Fold.initial, CachingStrategy.NoCache(), access)
        const resolve = () => Decider.forStream(category, StreamId.create(accountId.toString()), null)
        return new Service(resolve)
    }
}

const dynamoDB = new DynamoDB({ region: "local", endpoint: "http://localhost:8000" });
const client = new DynamoStoreClient(dynamoDB);
const context = new DynamoStoreContext({
  client,
  tableName: "equinox-js-example",
  tip: TipOptions.create({}),
  query: QueryOptions.create({}),
});


(async () => {
  const account = AccountId.create();
  const service = Service.create(account, context);
  await service.open(account, "Alice");
  await service.deposit(account, 100);
  await service.deposit(account, 100);
  await service.deposit(account, 200);
  await service.withdraw(account, 50);
  const myAccount = await service.readBalance(account);
  console.log(`${account} => ${myAccount.name} has ${myAccount.amount} dollars`);
})();