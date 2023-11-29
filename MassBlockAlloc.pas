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
  SysUtils,
  AuxTypes, AuxClasses, BitVector, BitOps;

{===============================================================================
    Library-specific exception
===============================================================================}
type
  EMBAException = class(Exception);

  EMBAInvalidValue     = class(EMBAException);
  EMBAInvalidState     = class(EMBAException);
  EMBAIndexOutOfBounds = class(EMBAException);
  EMBAInvalidAddress   = class(EMBAException);
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
    class Function CalculateMemorySize(BlockSize: TMemSize; MinBlockCount: Integer; MemoryAlignment: TMemoryAlignment = maNone): TMemSize; virtual;
    class Function CalculateBlockCount(BlockSize: TMemSize; MinBlockCount: Integer; MemoryAlignment: TMemoryAlignment = maNone): Integer; virtual;
    constructor Create(BlockSize: TMemSize; MinBlockCount: Integer; MemoryAlignment: TMemoryAlignment = maNone);
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

type
  TMBAShrinkMode = (smNormal,smNoShrink);
(*
  TMassBlockAlloc = class(TCustomListObject)
  protected
    fBlockSize:         TMemSize;
    fBlocksPerSegment:  Integer;
    fMemoryAlignment:   TMemoryAlignment;
    fSegments:          array of TMBASegment;

    //Function GetCapacity: Integer; override;
    //procedure SetCapacity(Value: Integer); override;
    //Function GetCount: Integer; override;
    //procedure SetCount(Value: Integer); override;

    procedure Initialize(BlockSize: TMemSize; MinBlocksPerSegment: Integer; MemoryAlignment: TMemoryAlignment); virtual;
    procedure Finalize; virtual;

  public
    class Function PredictSegmentSize(BlockSize: TMemSize; MinBlocksPerSegment: Integer; MemoryAlignment: TMemoryAlignment = maNone): TMemSize; virtual;
    constructor Create(BlockSize: TMemSize; MinBlocksPerSegment: Integer; MemoryAlignment: TMemoryAlignment = maNone);
    destructor Destroy; override;
    //Function LowIndex: Integer; override;
    //Function HighIndex: Integer; override;

    procedure Lock; virtual;
    procedure Unlock; virtual;    

    Function Allocate(InitMemory: Boolean = False): Pointer; overload; virtual;
    procedure Allocate(out Block: Pointer; InitMemory: Boolean = False); overload; virtual;
    procedure Allocate(out Blocks: array of Pointer; InitMemory: Boolean = False); overload; virtual;

    procedure Free(var Block: Pointer); virtual;
    procedure Free(var Blocks: array of Pointer); virtual;

    property BlockSize: TMemSize read fBlockSize;
    property BlocksPerSegment: Integer read fBlocksPerSegment;
    property MemoryAlignment: TMemoryAlignment read fMemoryAlignment;

    property TotalReservedMemory: TMemSize
    property TotalAllocatedMemory: TMemSize
    property TotalWastedMemory: TMemsize;
    property MemoryEffeciency: Double
    property MemoryUtilization: Double
    SegmentSize
    SegmentCount
    Segments
  end;
*)

implementation

uses
  {$IFDEF Windows}Windows,{$ENDIF} Math;

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
  _SC_PAGESIZE = 

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
// sysconf(_SC_PAGESIZE) or sysconf(_SC_PAGE_SIZE)
{$ENDIF}
end;

//------------------------------------------------------------------------------

class Function TMBASegment.CalculateMemorySize(BlockSize: TMemSize; MinBlockCount: Integer; MemoryAlignment: TMemoryAlignment = maNone): TMemSize;
var
  TempReservedBlockSize:  TMemSize;
  TempBlockCount:         Integer;
begin
Calculate(BlockSize,MinBlockCount,MemoryAlignment,TempReservedBlockSize,Result,TempBlockCount);
end;

//------------------------------------------------------------------------------

class Function TMBASegment.CalculateBlockCount(BlockSize: TMemSize; MinBlockCount: Integer; MemoryAlignment: TMemoryAlignment = maNone): Integer;
var
  TempReservedBlockSize:  TMemSize;
  TempMemorySize:         TMemSize;
begin
Calculate(BlockSize,MinBlockCount,MemoryAlignment,TempReservedBlockSize,TempMemorySize,Result);
end;

//------------------------------------------------------------------------------

constructor TMBASegment.Create(BlockSize: TMemSize; MinBlockCount: Integer; MemoryAlignment: TMemoryAlignment = maNone);
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
            raise EMBAInvalidState.CreateFmt('TMBASegment.TryAllocateBlockVector: Block %d already allocated.',[i]);
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
    // check if the vector is really allocated in full length
    ReqBlockCount := Ceil((TMemSize(VectorLength) * fBlockSize) / fReservedBlockSize);
    Index := BlockIndexOf(Vector);
    If CheckIndex(Index) then
      begin
        For i := Index to Pred(Index + ReqBlockCount) do
          If not fAllocationMap[i] then
            raise EMBAInvalidState.CreateFmt('TMBASegment.FreeBlockVector: Block %d not allocated.',[i]);
        For i := Index to Pred(Index + ReqBlockCount) do
          fAllocationMap[i] := False;
        Vector := nil;
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

end.
