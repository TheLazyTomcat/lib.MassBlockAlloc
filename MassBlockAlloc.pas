unit MassBlockAlloc;

{$IF Defined(WINDOWS) or Defined(MSWINDOWS)}
  {$DEFINE Windows}
{$ELSEIF Defined(LINUX) and Defined(FPC)}
  {$DEFINE Linux}
{$ELSE}
  {$MESSAGE FATAL 'Unsupported operating system.'}
{$IFEND}

{$IFDEF FPC}
  {$MODE ObjFPC}
  {$MODESWITCH DuplicateLocals+}
  {$DEFINE FPC_DisableWarns}
  {$MACRO ON}
{$ENDIF}
{$H+}

interface

uses
  SysUtils, SyncObjs,
  AuxTypes, AuxClasses, BitVector, BitOps;

{===============================================================================
    Library-specific exception
===============================================================================}
type
  EMBAException = class(Exception);

  EMBAInvalidValue     = class(EMBAException);
  EMBAInvalidState     = class(EMBAException);
  EMBAInvalidAddress   = class(EMBAException);
  EMBAInvalidAction    = class(EMBAException);
  EMBAIndexOutOfBounds = class(EMBAException);
  EMBAOutOfResources   = class(EMBAException);

{===============================================================================
--------------------------------------------------------------------------------
                                   TMBASegment
--------------------------------------------------------------------------------
===============================================================================}
{===============================================================================
    TMBASegment - class declaration
===============================================================================}
type
  TMBASegment = class(TCustomListObject)
  protected
    fBlockSize:         TMemSize;
    fReservedBlockSize: TMemSize;
    fBlockCount:        Integer;
    fMemory:            Pointer;
    fMemorySize:        TMemSize;
    fAllocationMap:     TBitVectorStatic;
    fLowAddress:        Pointer;  // address of first block
    fHighAddress:       Pointer;  // address BEHIND the last block (reserved size)
    // getters, setters
    Function GetIsAllocated(Index: Integer): Boolean; virtual;
    Function GetBlockAddress(Index: Integer): Pointer; virtual;
    // list methods
    Function GetCapacity: Integer; override;
    procedure SetCapacity(Value: Integer); override;
    Function GetCount: Integer; override;
    procedure SetCount(Value: Integer); override;
    // init/final
    procedure Initialize(BlockSize: TMemSize; MinBlockCount: Integer; MemoryAlignment: TMemoryAlignment); virtual;
    procedure Finalize; virtual;       
    // auxiliary methods and utilities
    class procedure Calculate(BlockSize: TMemSize; MinBlockCount: Integer; MemoryAlignment: TMemoryAlignment;
                              out ReservedBlockSize, MemorySize: TMemSize; out BlockCount: Integer); virtual;
    Function FindSpaceForVector(VectorLength: Integer; out ReqBlockCount: Integer): Integer; virtual;
  public
    class Function MemoryPageSize: TMemSize; virtual;
    class Function CalculateVectorReqBlockCount(BlockSize: TMemSize; VectorLength: Integer; MemoryAlignment: TMemoryAlignment): Integer; virtual;
    class Function CalculateMemorySize(BlockSize: TMemSize; MinBlockCount: Integer; MemoryAlignment: TMemoryAlignment): TMemSize; virtual;
    class Function CalculateBlockCount(BlockSize: TMemSize; MinBlockCount: Integer; MemoryAlignment: TMemoryAlignment): Integer; virtual;
    constructor Create(BlockSize: TMemSize; MinBlockCount: Integer; MemoryAlignment: TMemoryAlignment);
    destructor Destroy; override;
    Function LowIndex: Integer; override;
    Function HighIndex: Integer; override;
    // address checks
    Function AddressOwned(Address: Pointer; Strict: Boolean = False): Boolean; virtual;
    Function AddressIndexOf(Address: Pointer; Strict: Boolean = False): Integer; virtual;
    Function BlockOwned(Block: Pointer): Boolean; virtual;
    Function BlockIndexOf(Block: Pointer): Integer; virtual;
    // (de)allocation methods
    procedure AllocateBlock(out Block: Pointer; InitMemory: Boolean); virtual;
    procedure FreeBlock(var Block: Pointer); virtual;
    Function CanAllocateBlockVector(VectorLength: Integer): Boolean; virtual;
    Function TryAllocateBlockVector(out Vector: Pointer; VectorLength: Integer; InitMemory: Boolean): Boolean; virtual;
    procedure AllocateBlockVector(out Vector: Pointer; VectorLength: Integer; InitMemory: Boolean); virtual;
    procedure FreeBlockVector(var Vector: Pointer; VectorLength: Integer); virtual;
    // informative methods
    Function IsFull: Boolean; virtual;
    Function IsEmpty: Boolean; virtual;
    Function AllocatedBlockCount: Integer; virtual;
    Function FreeBlockCount: Integer; virtual;
    // memory statistics
    Function ReservedMemory: TMemSize; virtual;   // memory reserved for all blocks
    Function BlocksMemory: TMemSize; virtual;     // memory of all blocks (excluding padding, so only BlockCount * BlockSize)
    Function AllocatedMemory: TMemSize; virtual;  // memory of allocated blocks (excluding padding)
    Function WastedMemory: TMemSize; virtual;     // all padding (including individual blocks)
    Function MemoryEffeciency: Double; virtual;   // (MemorySize - WastedMemory) / MemorySize
    Function MemoryUtilization: Double; virtual;  // AllocatedMemory / BlocksMemory
    // properties
    property Capacity: Integer read GetCapacity;
    property Count: Integer read GetCount;
    property BlockSize: TMemSize read fBlockSize;
    property ReservedBlockSize: TMemSize read fReservedBlockSize;
    property BlockCount: Integer read fBlockCount;
    property Memory: Pointer read fMemory;
    property MemorySize: TMemSize read fMemorySize;
    property AllocationMap[Index: Integer]: Boolean read GetIsAllocated;
    property Blocks[Index: Integer]: Pointer read GetBlockAddress; default;
  end;

{===============================================================================
--------------------------------------------------------------------------------
                              TMassBlockAllocNoLock
--------------------------------------------------------------------------------
===============================================================================}
{===============================================================================
    TMassBlockAllocNoLock - class declaration
===============================================================================}
type
  TMassBlockAllocNoLock = class(TCustomListObject)
  protected
    fBlockSize:           TMemSize;
    fMinBlocksPerSegment: Integer;
    fMemoryAlignment:     TMemoryAlignment;
    fSegments:            array of TMBASegment;
    fSegmentCount:        Integer;
    fThreadLock:          TCriticalSection;
    // getters setters
    Function GetSegment(Index: Integer): TMBASegment; virtual;
    // inherited list methods
    Function GetCapacity: Integer; override;
    procedure SetCapacity(Value: Integer); override;
    Function GetCount: Integer; override;
    procedure SetCount(Value: Integer); override;
    // internal list management
    Function AddSegment: Integer; virtual;
    procedure DeleteSegment(Index: Integer); virtual;
    procedure ClearSegments; virtual;
    // init/final
    procedure Initialize(BlockSize: TMemSize; MinBlocksPerSegment: Integer; MemoryAlignment: TMemoryAlignment); virtual;
    procedure Finalize; virtual;
    // other internals
    Function InternalLowIndex: Integer; virtual;
    Function InternalHighIndex: Integer; virtual;
    procedure InternalLock; virtual;
    procedure InternalUnlock; virtual;
  public
    constructor Create(BlockSize: TMemSize; MinBlocksPerSegment: Integer; MemoryAlignment: TMemoryAlignment = maNone);
    destructor Destroy; override;
    Function LowIndex: Integer; override;
    Function HighIndex: Integer; override;
    // thread-locking facility
    procedure Lock; virtual;
    procedure Unlock; virtual;
    // main (de)allocation functions
    Function AllocateBlock(InitMemory: Boolean = False): Pointer; overload; virtual;
    procedure AllocateBlock(out Block: Pointer; InitMemory: Boolean = False); overload; virtual;
    procedure AllocateBlocks(out Blocks: array of Pointer; InitMemory: Boolean = False); virtual;
    procedure AllocateBlockVector(out Vector: Pointer; VectorLength: Integer; InitMemory: Boolean = False); virtual;
    procedure FreeBlock(var Block: Pointer); virtual;
    procedure FreeBlocks(var Blocks: array of Pointer); virtual;
    procedure FreeBlockVector(var Vector: Pointer; VectorLength: Integer); virtual;
    // informations and statistics
    Function SegmentSize: TMemSize; virtual;
    Function BlocksPerSegment: Integer; virtual;
    Function AllocatedBlockCount: Integer; virtual;
    Function FreeBlockCount: Integer; virtual;
    Function TotalReservedMemory: TMemSize; virtual;
    Function TotalBlocksMemory: TMemSize; virtual;
    Function TotalAllocatedMemory: TMemSize; virtual;
    Function TotalWastedMemory: TMemSize; virtual;
    Function TotalMemorySize: TMemSize; virtual;
    Function MemoryEffeciency: Double; virtual;
    Function MemoryUtilization: Double; virtual;
    // properties
    property BlockSize: TMemSize read fBlockSize;
    property MinBlocksPerSegment: Integer read fMinBlocksPerSegment;
    property MemoryAlignment: TMemoryAlignment read fMemoryAlignment;
    property Count: Integer read GetCount;
    property SegmentCount: Integer read GetCount;
    property Segments[Index: Integer]: TMBASegment read GetSegment; default;
  end;

{===============================================================================
--------------------------------------------------------------------------------
                                 TMassBlockAlloc
--------------------------------------------------------------------------------
===============================================================================}
{===============================================================================
    TMassBlockAlloc - class declaration
===============================================================================}
type
  TMassBlockAlloc = class(TMassBlockAllocNoLock)
  protected
    procedure InternalLock; override;
    procedure InternalUnlock; override;
  end;

{===============================================================================
--------------------------------------------------------------------------------
                              TMassBlockAllocCached                                                            
--------------------------------------------------------------------------------
===============================================================================}
{===============================================================================
    TMassBlockAllocCached - class declaration
===============================================================================}
type
  TMassBlockAllocCached = class(TMassBlockAlloc)
  protected
    fCacheLock:   TCriticalSection;
    fCacheEvent:  TEvent;
    fCacheThread: TObject;
    fCacheData: record
      
    end;
    //procedure Initialize(ChacheSize: Integer; BlockSize: TMemSize; MinBlocksPerSegment: Integer; MemoryAlignment: TMemoryAlignment); virtual;
    //procedure Finalize; virtual;

    //procedure LockCache; virtual;
    //procedure UnlockCache; virtual;


  public
    //constructor Create(ChacheSize: Integer; BlockSize: TMemSize; MinBlocksPerSegment: Integer; MemoryAlignment: TMemoryAlignment = maNone);
  end;

implementation

uses
  {$IFDEF Windows}Windows,{$ELSE}baseunix,{$ENDIF} Math;

{$IFDEF FPC_DisableWarns}
  {$DEFINE FPCDWM}
  {$DEFINE W4055:={$WARN 4055 OFF}} // Conversion between ordinals and pointers is not portable}
  {$DEFINE W5024:={$WARN 5024 OFF}} // Parameter "$1" not used
{$ENDIF}

{===============================================================================
    Externals
===============================================================================}

{$IFDEF Windows}

{$ELSE}

Function sysconf(name: cInt): cLong; cdecl; external;

const
  _SC_PAGESIZE = 30;  {$message 'check this value'} 

{$ENDIF}

{===============================================================================
    Helper constants, functions, ...
===============================================================================}
const
  OneGiB = 1024 * 1024 * 1024;  // one gibibyte (2^30, gigabyte for you oldschools :P)

//------------------------------------------------------------------------------  

{$IF not Declared(Ceil64)}
Function Ceil64(x: Extended): Int64;
begin
Result := Trunc(x);
If Frac(x) > 0 then
  Result := Result + 1;
end;
{$IFEND}

{===============================================================================
--------------------------------------------------------------------------------
                                   TMBASegment
--------------------------------------------------------------------------------
===============================================================================}
{===============================================================================
    TMBASegment - class implementation
===============================================================================}
{-------------------------------------------------------------------------------
    TMBASegment - protected methods
-------------------------------------------------------------------------------}

Function TMBASegment.GetIsAllocated(Index: Integer): Boolean;
begin
If CheckIndex(Index) then
  Result := fAllocationMap[Index]
else
  raise EMBAIndexOutOfBounds.CreateFmt('TMBASegment.GetIsAllocated: Index (%d) out of bounds.',[Index]);
end;

//------------------------------------------------------------------------------

Function TMBASegment.GetBlockAddress(Index: Integer): Pointer;
begin
If CheckIndex(Index) then
  Result := PtrAdvance(fLowAddress,Index,fReservedBlockSize)
else
  raise EMBAIndexOutOfBounds.CreateFmt('TMBASegment.GetBlockAddress: Index (%d) out of bounds.',[Index]);
end;

//------------------------------------------------------------------------------

Function TMBASegment.GetCapacity: Integer;
begin
Result := fBlockCount;
end;

//------------------------------------------------------------------------------

{$IFDEF FPCDWM}{$PUSH}W5024{$ENDIF}
procedure TMBASegment.SetCapacity(Value: Integer);
begin
// do nothing
end;
{$IFDEF FPCDWM}{$POP}{$ENDIF}

//------------------------------------------------------------------------------

Function TMBASegment.GetCount: Integer;
begin
Result := fBlockCount;
end;

//------------------------------------------------------------------------------

{$IFDEF FPCDWM}{$PUSH}W5024{$ENDIF}
procedure TMBASegment.SetCount(Value: Integer);
begin
// do nothing
end;
{$IFDEF FPCDWM}{$POP}{$ENDIF}

//------------------------------------------------------------------------------

procedure TMBASegment.Initialize(BlockSize: TMemSize; MinBlockCount: Integer; MemoryAlignment: TMemoryAlignment);
begin
fBlockSize := BlockSize;
Calculate(BlockSize,MinBlockCount,MemoryAlignment,fReservedBlockSize,fMemorySize,fBlockCount);
GetMem(fMemory,fMemorySize);
fAllocationMap := TBitVectorStatic.Create(fMemory,fBlockCount);
fAllocationMap.Fill(False);
fLowAddress := AlignedMemory(PtrAdvance(fMemory,fAllocationMap.MemorySize),MemoryAlignment);
fHighAddress := PtrAdvance(fLowAddress,PtrInt(TMemSize(fBlockCount) * fReservedBlockSize));
end;

//------------------------------------------------------------------------------

procedure TMBASegment.Finalize;
begin
If Assigned(fAllocationMap) then
  If not fAllocationMap.IsEmpty then
    raise EMBAInvalidState.CreateFmt('TMBASegment.Finalize: Not all blocks were freed (%d).',[fAllocationMap.PopCount]);
fAllocationMap.Free;
FreeMem(fMemory,fMemorySize);
end;

//------------------------------------------------------------------------------

class procedure TMBASegment.Calculate(BlockSize: TMemSize; MinBlockCount: Integer; MemoryAlignment: TMemoryAlignment;
                                      out ReservedBlockSize, MemorySize: TMemSize; out BlockCount: Integer);
var
  PredAlignBytes: TMemSize;
  PageSize:       TMemSize;
begin
If BlockSize > 0 then
  begin
    If MinBlockCount > 0 then
      begin
        PredAlignBytes := Pred(AlignmentBytes(MemoryAlignment));
        ReservedBlockSize := (BlockSize + PredAlignBytes) and not PredAlignBytes;
        // sanity check
        If (Int64(ReservedBlockSize) * Int64(MinBlockCount)) <= OneGiB then
          begin
            PageSize := MemoryPageSize;          
          {
            memory size...

              P ... page size
              A ... alignment bytes

              m ... initial memory size
              n ... initial size of allocation map with padding
              c ... min. block count
              b ... block size

              M ... memory size (result)
              B ... reserved block size

                M = ceil(m / P) * P

                  m = n + (c * B)

                    n = ceil(ceil(c / 8) / A) * A = (((c + 7) shr 3) + pred(A)) and not pred(A)

                    B = ceil(b / A) * A = (b + Pred(A)) and not Pred(A)
          }
            MemorySize := TMemSize(Ceil64((
                ((((TMemSize(MinBlockCount) + 7) shr 3) + PredAlignBytes) and not PredAlignBytes) + // allocation map with padding
                (TMemSize(MinBlockCount) * ReservedBlockSize)) / PageSize)) * PageSize;             // ensure page-size granularity
          {
            block count...

              A ... alignment bytes

              M ... memory size
              C ... block count (result)
              B ... reserved block size

                  M = (ceil(C / 8A) * A) + CB
                M/A = ceil(C / 8A) + (CB / A)

                M/A >= (C / 8A) + (CB / A)
                M/A >= (C / 8A) + (8CB / 8A)
                M/A >= (8CB + C) / 8A
                 8M >= C * (8B + 1)
                  C <= 8M / (8B + 1)

                  C = floor(8M / (8B + 1))
          }
            BlockCount := Floor((8 * MemorySize) / ((8 * ReservedBlockSize) + 1));
          end
        else raise EMBAInvalidValue.CreateFmt('TMBASegment.Calculate: Segment too large (%d).',[Int64(ReservedBlockSize) * Int64(MinBlockCount)]);
      end
    else raise EMBAInvalidValue.CreateFmt('TMBASegment.Calculate: Invalid minimal block count (%d).',[MinBlockCount]);
  end
else raise EMBAInvalidValue.CreateFmt('TMBASegment.Calculate: Invalid block size (%u).',[BlockSize]);
end;

//------------------------------------------------------------------------------

Function TMBASegment.FindSpaceForVector(VectorLength: Integer; out ReqBlockCount: Integer): Integer;
var
  i,j:      Integer;
  SeqFound: Boolean;
begin
If VectorLength > 1 then
  begin
  {
    Find out how many consecutive blocks is needed. It might be less than
    VectorLength, because we will also use the block padding (if any exists).
  }
    ReqBlockCount := Ceil((TMemSize(VectorLength) * fBlockSize) / fReservedBlockSize);
    If ReqBlockCount > 1 then
      begin
        Result := -1;
        // check if there is required number of free blocks
        If FreeBlockCount >= ReqBlockCount then
          begin
            // find if there is consecutive sequence of free blocks of required length
            For i := fAllocationMap.FirstClean to (fAllocationMap.Count - ReqBlockCount) do
              If not fAllocationMap[i] then
                begin
                  SeqFound := True;
                  For j := Succ(i){because i is already checked} to Pred(i + ReqBlockCount) do
                    If fAllocationMap[j] then
                      begin
                        SeqFound := False;
                        Break{For j};
                      end;
                  If SeqFound then
                    begin
                      Result := i;
                      Break{For i};
                    end;
                end;
          end;
      end
    else Result := fAllocationMap.FirstClean;
  end
else If VectorLength > 0 then
  begin
    ReqBlockCount := 1;
    Result := fAllocationMap.FirstClean;
  end
else raise EMBAInvalidValue.CreateFmt('TMBASegment.FindSpaceForVector: Invalid vector length (%d).',[VectorLength]);
end;

{-------------------------------------------------------------------------------
    TMBASegment - public methods
-------------------------------------------------------------------------------}

class Function TMBASegment.MemoryPageSize: TMemSize;
{$IFDEF Windows}
var
  SystemInfo: TSystemInfo;
begin
{$IFDEF FPC}
SystemInfo := Default(TSystemInfo);
{$ENDIF}
GetSystemInfo(SystemInfo);
Result := TMemSize(SystemInfo.dwPageSize);
{$ELSE}
begin
Result := TMemSize(sysconf(_SC_PAGESIZE));
{$message 'deal with errors'}
{$ENDIF}
end;

//------------------------------------------------------------------------------

class Function TMBASegment.CalculateVectorReqBlockCount(BlockSize: TMemSize; VectorLength: Integer; MemoryAlignment: TMemoryAlignment): Integer;
var
  PredAlignBytes: TMemSize;
begin
PredAlignBytes := Pred(AlignmentBytes(MemoryAlignment));
Result := Ceil((TMemSize(VectorLength) * BlockSize) / ((BlockSize + PredAlignBytes) and not PredAlignBytes));
end;

//------------------------------------------------------------------------------

class Function TMBASegment.CalculateMemorySize(BlockSize: TMemSize; MinBlockCount: Integer; MemoryAlignment: TMemoryAlignment): TMemSize;
var
  TempReservedBlockSize:  TMemSize;
  TempBlockCount:         Integer;
begin
Calculate(BlockSize,MinBlockCount,MemoryAlignment,TempReservedBlockSize,Result,TempBlockCount);
end;

//------------------------------------------------------------------------------

class Function TMBASegment.CalculateBlockCount(BlockSize: TMemSize; MinBlockCount: Integer; MemoryAlignment: TMemoryAlignment): Integer;
var
  TempReservedBlockSize:  TMemSize;
  TempMemorySize:         TMemSize;
begin
Calculate(BlockSize,MinBlockCount,MemoryAlignment,TempReservedBlockSize,TempMemorySize,Result);
end;

//------------------------------------------------------------------------------

constructor TMBASegment.Create(BlockSize: TMemSize; MinBlockCount: Integer; MemoryAlignment: TMemoryAlignment);
begin
inherited Create;
Initialize(BlockSize,MinBlockCount,MemoryAlignment);
end;

//------------------------------------------------------------------------------

destructor TMBASegment.Destroy;
begin
Finalize;
inherited;
end;

//------------------------------------------------------------------------------

Function TMBASegment.LowIndex: Integer;
begin
Result := 0;
end;

//------------------------------------------------------------------------------

Function TMBASegment.HighIndex: Integer;
begin
Result := Pred(fBlockCount);
end;

//------------------------------------------------------------------------------

Function TMBASegment.AddressOwned(Address: Pointer; Strict: Boolean = False): Boolean;
var
  Index:  Integer;
begin
Result := False;
{$IFDEF FPCDWM}{$PUSH}W4055{$ENDIF}
If (PtrUInt(Address) >= PtrUInt(fLowAddress)) and (PtrUInt(Address) < PtrUInt(fHighAddress)) then
  begin
    If Strict then
      begin
        // get index of block to which the address points
        Index := Integer((PtrUInt(Address) - PtrUInt(fLowAddress)) div PtrUInt(fReservedBlockSize));
        If CheckIndex(Index) then
          // check whether the address is within the block size (NOT reserved block size)
          Result := (PtrUInt(Address) - PtrUInt(PtrAdvance(fLowAddress,Index,fReservedBlockSize))) < fBlockSize;
      end
    else Result := True;
  end;
{$IFDEF FPCDWM}{$POP}{$ENDIF}
end;

//------------------------------------------------------------------------------

Function TMBASegment.AddressIndexOf(Address: Pointer; Strict: Boolean = False): Integer;
var
  Index:  Integer;
begin
Result := -1;
{$IFDEF FPCDWM}{$PUSH}W4055{$ENDIF}
If (PtrUInt(Address) >= PtrUInt(fLowAddress)) and (PtrUInt(Address) < PtrUInt(fHighAddress)) then
  begin
    Index := Integer((PtrUInt(Address) - PtrUInt(fLowAddress)) div PtrUInt(fReservedBlockSize));
    If CheckIndex(Index) then
      If not Strict or ((PtrUInt(Address) - PtrUInt(PtrAdvance(fLowAddress,Index,fReservedBlockSize))) < fBlockSize) then
        Result := Index;
  end;
{$IFDEF FPCDWM}{$POP}{$ENDIF}
end;

//------------------------------------------------------------------------------

Function TMBASegment.BlockOwned(Block: Pointer): Boolean;
var
  Index:  Integer;
begin
Result := False;
{$IFDEF FPCDWM}{$PUSH}W4055{$ENDIF}
If (PtrUInt(Block) >= PtrUInt(fLowAddress)) and (PtrUInt(Block) < PtrUInt(fHighAddress)) then
  begin
    Index := Integer((PtrUInt(Block) - PtrUInt(fLowAddress)) div PtrUInt(fReservedBlockSize));
    If CheckIndex(Index) then
      Result := Block = PtrAdvance(fLowAddress,Index,fReservedBlockSize);
  end;
{$IFDEF FPCDWM}{$POP}{$ENDIF}
end;

//------------------------------------------------------------------------------

Function TMBASegment.BlockIndexOf(Block: Pointer): Integer;
var
  Index:  Integer;
begin
Result := -1;
{$IFDEF FPCDWM}{$PUSH}W4055{$ENDIF}
If (PtrUInt(Block) >= PtrUInt(fLowAddress)) and (PtrUInt(Block) < PtrUInt(fHighAddress)) then
  begin
    Index := Integer((PtrUInt(Block) - PtrUInt(fLowAddress)) div PtrUInt(fReservedBlockSize));
    If CheckIndex(Index) and (Block = PtrAdvance(fLowAddress,Index,fReservedBlockSize)) then
      Result := Index;
  end;
{$IFDEF FPCDWM}{$POP}{$ENDIF}
end;

//------------------------------------------------------------------------------

procedure TMBASegment.AllocateBlock(out Block: Pointer; InitMemory: Boolean);
var
  Index:  Integer;
begin
If not fAllocationMap.IsFull then
  begin
    Index := fAllocationMap.FirstClean;
    If CheckIndex(Index) then
      begin
        // paranoia check
        If fAllocationMap[Index] then
          raise EMBAInvalidState.CreateFmt('TMBASegment.AllocateBlock: Block %d already allocated.',[Index]);
        fAllocationMap[Index] := True;
        Block := PtrAdvance(fLowAddress,Index,fReservedBlockSize);
        If InitMemory then
          FillChar(Block^,fReservedBlockSize{yes, better clear it all},0);
      end
    else raise EMBAOutOfResources.Create('TMBASegment.AllocateBlock: Unable to allocate block.');
  end
else raise EMBAOutOfResources.Create('TMBASegment.AllocateBlock: No free block to allocate.');
end;

//------------------------------------------------------------------------------

procedure TMBASegment.FreeBlock(var Block: Pointer);
var
  Index:  Integer;
begin
Index := BlockIndexOf(Block);
If CheckIndex(Index) then
  begin
    If not fAllocationMap[Index] then
      raise EMBAInvalidState.CreateFmt('TMBASegment.FreeBlock: Block %d not allocated.',[Index]);
    fAllocationMap[Index] := False;
    Block := nil;
  end
else raise EMBAInvalidAddress.CreateFmt('TMBASegment.FreeBlock: Invalid address (%p).',[Block]);
end;

//------------------------------------------------------------------------------

Function TMBASegment.CanAllocateBlockVector(VectorLength: Integer): Boolean;
var
  ReqBlockCount:  Integer;
begin
Result := CheckIndex(FindSpaceForVector(VectorLength,ReqBlockCount));
end;

//------------------------------------------------------------------------------

Function TMBASegment.TryAllocateBlockVector(out Vector: Pointer; VectorLength: Integer; InitMemory: Boolean): Boolean;
var
  ReqBlockCount:  Integer;
  Index,i:        Integer;
begin
Result := False;
If VectorLength > 0 then
  begin
    Index := FindSpaceForVector(VectorLength,ReqBlockCount);
    If CheckIndex(Index) and (ReqBlockCount > 0) then
      begin
        // let's be super paranoid...
        For i := Index to Pred(Index + ReqBlockCount) do
          If fAllocationMap[i] then
            Exit;
         For i := Index to Pred(Index + ReqBlockCount) do
          fAllocationMap[i] := True;
        Vector := PtrAdvance(fLowAddress,Index,fReservedBlockSize);
        If InitMemory then
          FillChar(Vector^,TMemSize(ReqBlockCount) * fReservedBlockSize,0);
        Result := True;
      end;
  end;
end;

//------------------------------------------------------------------------------

procedure TMBASegment.AllocateBlockVector(out Vector: Pointer; VectorLength: Integer; InitMemory: Boolean);
begin
If VectorLength > 0 then
  begin
    If not TryAllocateBlockVector(Vector,VectorLength,InitMemory) then
      raise EMBAOutOfResources.CreateFmt('TMBASegment.AllocateBlockVector: Unable to allocate vector of length %d.',[VectorLength]);
  end
else raise EMBAInvalidValue.CreateFmt('TMBASegment.AllocateBlockVector: Invalid vector length (%d).',[VectorLength]);
end;

//------------------------------------------------------------------------------

procedure TMBASegment.FreeBlockVector(var Vector: Pointer; VectorLength: Integer);
var
  ReqBlockCount:  Integer;
  Index,i:        Integer;
begin
If VectorLength > 0 then
  begin
    Index := BlockIndexOf(Vector);
    If CheckIndex(Index) then
      begin
        // check if the vector is really allocated in full length
        ReqBlockCount := Ceil((TMemSize(VectorLength) * fBlockSize) / fReservedBlockSize);
        If ReqBlockCount <= (fBlockCount - Index) then
          begin
            For i := Index to Pred(Index + ReqBlockCount) do
              If not fAllocationMap[i] then
                raise EMBAInvalidState.CreateFmt('TMBASegment.FreeBlockVector: Block %d not allocated.',[i]);
            For i := Index to Pred(Index + ReqBlockCount) do
              fAllocationMap[i] := False;
            Vector := nil;
          end
        else raise EMBAInvalidValue.CreateFmt('TMBASegment.FreeBlockVector: Vector too long (%d(%d)).',[VectorLength,ReqBlockCount]);
      end
    else raise EMBAInvalidAddress.CreateFmt('TMBASegment.FreeBlockVector: Invalid address (%p).',[Vector]);
  end
else raise EMBAInvalidValue.CreateFmt('TMBASegment.FreeBlockVector: Invalid vector length (%d).',[VectorLength]);
end;

//------------------------------------------------------------------------------

Function TMBASegment.IsFull: Boolean;
begin
Result := fAllocationMap.IsFull;
end;

//------------------------------------------------------------------------------

Function TMBASegment.IsEmpty: Boolean;
begin
Result := fAllocationMap.IsEmpty;
end;

//------------------------------------------------------------------------------

Function TMBASegment.AllocatedBlockCount: Integer;
begin
Result := fAllocationMap.PopCount;
end;

//------------------------------------------------------------------------------

Function TMBASegment.FreeBlockCount: Integer;
begin
Result := fAllocationMap.Count - fAllocationMap.PopCount;
end;

//------------------------------------------------------------------------------

Function TMBASegment.ReservedMemory: TMemSize;
begin
Result := TMemSize(fBlockCount) * fReservedBlockSize;
end;

//------------------------------------------------------------------------------

Function TMBASegment.BlocksMemory: TMemSize;
begin
Result := TMemSize(fBlockCount) * fBlockSize;
end;

//------------------------------------------------------------------------------

Function TMBASegment.AllocatedMemory: TMemSize;
begin
Result := TMemSize(AllocatedBlockCount) * fBlockSize;
end;

//------------------------------------------------------------------------------

Function TMBASegment.WastedMemory: TMemSize;
begin
Result := fMemorySize - BlocksMemory - fAllocationMap.MemorySize;
end;

//------------------------------------------------------------------------------

Function TMBASegment.MemoryEffeciency: Double;
begin
If fMemorySize <> 0 then
  Result := (fMemorySize - WastedMemory) / fMemorySize
else
  Result := 0.0;
end;

//------------------------------------------------------------------------------

Function TMBASegment.MemoryUtilization: Double;
begin
If BlocksMemory <> 0 then
  Result := AllocatedMemory / BlocksMemory
else
  Result := 0.0;
end;


{===============================================================================
--------------------------------------------------------------------------------
                              TMassBlockAllocNoLock
--------------------------------------------------------------------------------
===============================================================================}
{===============================================================================
    TMassBlockAllocNoLock - class implementation
===============================================================================}
{-------------------------------------------------------------------------------
    TMassBlockAllocNoLock - protected methods
-------------------------------------------------------------------------------}

Function TMassBlockAllocNoLock.GetSegment(Index: Integer): TMBASegment;
begin
If CheckIndex(Index) then
  Result := fSegments[Index]
else
  raise EMBAIndexOutOfBounds.CreateFmt('TMassBlockAllocNoLock.GetSegment: Index (%d) out of bounds.',[Index]);
end;

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.GetCapacity: Integer;
begin
Result := Length(fSegments);
end;

//------------------------------------------------------------------------------

procedure TMassBlockAllocNoLock.SetCapacity(Value: Integer);
begin
If Value >= 0 then
  begin
    If Value <> Length(fSegments) then
      begin
        If Value < fSegmentCount then
          raise EMBAInvalidAction.Create('TMassBlockAllocNoLock.SetCapacity: Cannot lower capacity below count.');
        SetLength(fSegments,Value);
      end;
  end
else raise EMBAInvalidValue.CreateFmt('TMassBlockAllocNoLock.SetCapacity: Invalid capacity (%d).',[Value]);
end;

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.GetCount: Integer;
begin
Result := fSegmentCount;
end;

//------------------------------------------------------------------------------

{$IFDEF FPCDWM}{$PUSH}W5024{$ENDIF}
procedure TMassBlockAllocNoLock.SetCount(Value: Integer);
begin
// do nothing
end;
{$IFDEF FPCDWM}{$POP}{$ENDIF}

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.AddSegment: Integer;
begin
Grow;
Result := fSegmentCount;
fSegments[Result] := TMBASegment.Create(fBlockSize,fMinBlocksPerSegment,fMemoryAlignment);
Inc(fSegmentCount);
end;

//------------------------------------------------------------------------------

procedure TMassBlockAllocNoLock.DeleteSegment(Index: Integer);
var
  i:  Integer;
begin
If CheckIndex(Index) then
  begin
    fSegments[Index].Free;
    For i := Index to Pred(InternalHighIndex) do
      fSegments[i] := fSegments[i + 1];
    fSegments[InternalHighIndex] := nil;
    Dec(fSegmentCount);
    Shrink;
  end
else raise EMBAIndexOutOfBounds.CreateFmt('TMassBlockAllocNoLock.DeleteSegment: Index (%d) out of bounds.',[Index]);
end;

//------------------------------------------------------------------------------

procedure TMassBlockAllocNoLock.ClearSegments;
var
  i:  Integer;
begin
For i := InternalLowIndex to InternalHighIndex do
  fSegments[i].Free;
SetLength(fSegments,0);
fSegmentCount := 0;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAllocNoLock.Initialize(BlockSize: TMemSize; MinBlocksPerSegment: Integer; MemoryAlignment: TMemoryAlignment);
begin
fBlockSize := BlockSize;
fMinBlocksPerSegment := MinBlocksPerSegment;
fMemoryAlignment := MemoryAlignment;
SetLength(fSegments,0);
fSegmentCount := 0;
fThreadLock := SyncObjs.TCriticalSection.Create;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAllocNoLock.Finalize;
begin
FreeAndNil(fThreadLock);
ClearSegments;
end;

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.InternalLowIndex: Integer;
begin
Result := Low(fSegments);
end;

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.InternalHighIndex: Integer;
begin
Result := Pred(fSegmentCount);
end;

//------------------------------------------------------------------------------

procedure TMassBlockAllocNoLock.InternalLock;
begin
// do nothing
end;

//------------------------------------------------------------------------------

procedure TMassBlockAllocNoLock.InternalUnlock;
begin
// do nothing
end;

{-------------------------------------------------------------------------------
    TMassBlockAllocNoLock - public methods
-------------------------------------------------------------------------------}

constructor TMassBlockAllocNoLock.Create(BlockSize: TMemSize; MinBlocksPerSegment: Integer; MemoryAlignment: TMemoryAlignment = maNone);
begin
inherited Create;
Initialize(BlockSize,MinBlocksPerSegment,MemoryAlignment);
end;

//------------------------------------------------------------------------------

destructor TMassBlockAllocNoLock.Destroy;
begin
Finalize;
inherited;
end;

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.LowIndex: Integer;
begin
InternalLock;
try
  Result := Low(fSegments);
finally
  InternalUnlock;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.HighIndex: Integer;
begin
InternalLock;
try
  Result := Pred(fSegmentCount);
finally
  InternalUnlock;
end;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAllocNoLock.Lock;
begin
fThreadLock.Enter;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAllocNoLock.Unlock;
begin
fThreadLock.Leave;
end;

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.AllocateBlock(InitMemory: Boolean = False): Pointer;
begin
AllocateBlock(Result,InitMemory);
end;

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

procedure TMassBlockAllocNoLock.AllocateBlock(out Block: Pointer; InitMemory: Boolean = False);
var
  i:  Integer;
begin
InternalLock;
try
  // first try to allocate in existing segments
  For i := InternalLowIndex to InternalHighIndex do
    If not fSegments[i].IsFull then
      begin
        fSegments[i].AllocateBlock(Block,InitMemory);
        Exit;
      end;
  // no free block in existing segments, add new one
  fSegments[AddSegment].AllocateBlock(Block,InitMemory);
finally
  InternalUnlock;
end;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAllocNoLock.AllocateBlocks(out Blocks: array of Pointer; InitMemory: Boolean = False);
var
  Index:  Integer;
  i:      Integer;
begin
InternalLock;
try
  If Length(Blocks) > 0 then
    begin
      Index := Low(Blocks);
      For i := InternalLowIndex to InternalHighIndex do
        while not fSegments[i].IsFull do
          begin
            fSegments[i].AllocateBlock(Blocks[Index],InitMemory);
            If Index < High(Blocks) then
              Inc(Index)
            else
              Exit;
          end;
      while Index <= High(Blocks) do
        begin
          i := AddSegment;
          while not fSegments[i].IsFull do
            begin
              fSegments[i].AllocateBlock(Blocks[Index],InitMemory);
              If Index < High(Blocks) then
                Inc(Index)
              else
                Exit;
            end;
        end;
    end;
finally
  InternalUnlock;
end;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAllocNoLock.AllocateBlockVector(out Vector: Pointer; VectorLength: Integer; InitMemory: Boolean = False);
var
  i:  Integer;
begin
InternalLock;
try
  If VectorLength > 0 then
    begin
      // check whether the vector can fit into any segment
      If TMBASegment.CalculateVectorReqBlockCount(fBlockSize,VectorLength,fMemoryAlignment) <= BlocksPerSegment then
        begin
          For i := InternalLowIndex to InternalHighIndex do
            If fSegments[i].TryAllocateBlockVector(Vector,VectorLength,InitMemory) then
              Exit;
          // vector not allocated in existing segments, add a new one and allocate there
          fSegments[AddSegment].AllocateBlockVector(Vector,VectorLength,InitMemory);
        end
      else raise EMBAOutOfResources.CreateFmt('TMassBlockAllocNoLock.AllocateBlockVector: Vector is too long (%d).',[VectorLength]);
    end
  else raise EMBAInvalidValue.CreateFmt('TMassBlockAllocNoLock.AllocateBlockVector: Invalid vector length (%d).',[VectorLength]);
finally
  InternalUnlock;
end;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAllocNoLock.FreeBlock(var Block: Pointer);
var
  i:  Integer;
begin
InternalLock;
try
  For i := InternalHighIndex downto InternalLowIndex do
    If fSegments[i].BlockOwned(Block) then
      begin
        fSegments[i].FreeBlock(Block);
        If fSegments[i].IsEmpty then
          DeleteSegment(i);
        Exit;
      end;
  raise EMBAInvalidAddress.CreateFmt('TMassBlockAllocNoLock.FreeBlock: Unable to free block (%p).',[Block]);
finally
  InternalUnlock;
end;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAllocNoLock.FreeBlocks(var Blocks: array of Pointer);
var
  Index,i:  Integer;
  Counter:  Integer;
begin
InternalLock;
try
  If Length(Blocks) > 0 then
    begin
      Counter := Length(Blocks);
      For i := InternalHighIndex downto InternalLowIndex do
        begin
          For Index := Low(Blocks) to High(Blocks) do
            If Assigned(Blocks[Index]) and fSegments[i].BlockOwned(Blocks[Index]) then
              begin
                fSegments[i].FreeBlock(Blocks[Index]);
                Dec(Counter);
                If Counter <= 0 then
                  begin
                    If fSegments[i].IsEmpty then
                      DeleteSegment(i);
                    Exit;
                  end
              end;
          If fSegments[i].IsEmpty then
            DeleteSegment(i);
        end;
      If Counter > 0 then
        raise EMBAInvalidAddress.CreateFmt('TMassBlockAllocNoLock.FreeBlocks: Not all blocks were freed (%d).',[Counter]);
    end;
finally
  InternalUnlock;
end;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAllocNoLock.FreeBlockVector(var Vector: Pointer; VectorLength: Integer);
var
  i:  Integer;
begin
InternalLock;
try
  If VectorLength > 0 then
    begin
      If TMBASegment.CalculateVectorReqBlockCount(fBlockSize,VectorLength,fMemoryAlignment) <= BlocksPerSegment then
        begin
          For i := InternalHighIndex downto InternalLowIndex do
            If fSegments[i].BlockOwned(Vector) then
              begin
                fSegments[i].FreeBlockVector(Vector,VectorLength);
                If fSegments[i].IsEmpty then
                  DeleteSegment(i);
                Exit;
              end;
          raise EMBAInvalidAddress.CreateFmt('TMassBlockAllocNoLock.FreeBlockVector: Unable to free vector (%p).',[Vector]);
        end
      else raise EMBAOutOfResources.CreateFmt('TMassBlockAllocNoLock.FreeBlockVector: Vector is too long (%d).',[VectorLength]);
    end
  else raise EMBAInvalidValue.CreateFmt('TMassBlockAllocNoLock.FreeBlockVector: Invalid vector length (%d).',[VectorLength]);
finally
  InternalUnlock;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.SegmentSize: TMemSize;
begin
InternalLock;
try
  If fSegmentCount > 0 then
    Result := fSegments[InternalLowIndex].MemorySize
  else
    Result := TMBASegment.CalculateMemorySize(fBlockSize,fMinBlocksPerSegment,fMemoryAlignment);
finally
  InternalUnlock;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.BlocksPerSegment: Integer;
begin
InternalLock;
try
  If fSegmentCount > 0 then
    Result := fSegments[InternalLowIndex].BlockCount
  else
    Result := TMBASegment.CalculateBlockCount(fBlockSize,fMinBlocksPerSegment,fMemoryAlignment);
finally
  InternalUnlock;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.AllocatedBlockCount: Integer;
var
  i:  Integer;
begin
InternalLock;
try
  Result := 0;
  For i := InternalLowIndex to InternalHighIndex do
    Inc(Result,fSegments[i].AllocatedBlockCount);
finally
  InternalUnlock;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.FreeBlockCount: Integer;
var
  i:  Integer;
begin
InternalLock;
try
  Result := 0;
  For i := InternalLowIndex to InternalHighIndex do
    Inc(Result,fSegments[i].FreeBlockCount);
finally
  InternalUnlock;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.TotalReservedMemory: TMemSize;
var
  i:  Integer;
begin
InternalLock;
try
  Result := 0;
  For i := InternalLowIndex to InternalHighIndex do
    Inc(Result,fSegments[i].ReservedMemory);
finally
  InternalUnlock;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.TotalBlocksMemory: TMemSize;
var
  i:  Integer;
begin
InternalLock;
try
  Result := 0;
  For i := InternalLowIndex to InternalHighIndex do
    Inc(Result,fSegments[i].BlocksMemory);
finally
  InternalUnlock;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.TotalAllocatedMemory: TMemSize;
var
  i:  Integer;
begin
InternalLock;
try
  Result := 0;
  For i := InternalLowIndex to InternalHighIndex do
    Inc(Result,fSegments[i].AllocatedMemory);
finally
  InternalUnlock;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.TotalWastedMemory: TMemSize;
var
  i:  Integer;
begin
InternalLock;
try
  Result := 0;
  For i := InternalLowIndex to InternalHighIndex do
    Inc(Result,fSegments[i].WastedMemory);
finally
  InternalUnlock;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.TotalMemorySize: TMemSize;
var
  i:  Integer;
begin
InternalLock;
try
  Result := 0;
  For i := InternalLowIndex to InternalHighIndex do
    Inc(Result,fSegments[i].MemorySize);
finally
  InternalUnlock;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.MemoryEffeciency: Double;
var
  MemorySize: TMemSize;
begin
InternalLock;
try
  MemorySize := TotalMemorySize;
  If MemorySize <> 0 then
    Result := (MemorySize - TotalWastedMemory) / MemorySize
  else
    Result := 0.0;
finally
  InternalUnlock;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAllocNoLock.MemoryUtilization: Double;
var
  BlocksMemory: TMemSize;
begin
InternalLock;
try
  BlocksMemory := TotalBlocksMemory;
  If BlocksMemory <> 0 then
    Result := TotalAllocatedMemory / BlocksMemory
  else
    Result := 0.0;
finally
  InternalUnlock;
end;
end;


{===============================================================================
--------------------------------------------------------------------------------
                                 TMassBlockAlloc
--------------------------------------------------------------------------------
===============================================================================}
{===============================================================================
    TMassBlockAlloc - class implementation
===============================================================================}
{-------------------------------------------------------------------------------
    TMassBlockAlloc - protected methods
-------------------------------------------------------------------------------}

procedure TMassBlockAlloc.InternalLock;
begin
fThreadLock.Enter;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.InternalUnlock;
begin
fThreadLock.Leave;
end;


{===============================================================================
--------------------------------------------------------------------------------
                              TMassBlockAllocCached                                                            
--------------------------------------------------------------------------------
===============================================================================}
{===============================================================================
    TMassBlockAllocCached - class implementation
===============================================================================}
{-------------------------------------------------------------------------------
    TMassBlockAllocCached - protected methods
-------------------------------------------------------------------------------}
{-------------------------------------------------------------------------------
    TMassBlockAllocCached - public methods
-------------------------------------------------------------------------------}

//constructor TMassBlockAllocCached.Create(ChacheSize: Integer; BlockSize: TMemSize; MinBlocksPerSegment: Integer; MemoryAlignment: TMemoryAlignment = maNone);
//begin
//end;

end.
